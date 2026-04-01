# Migrating a stackql backend to extended query support

This document describes how to add extended query protocol support to a stackql `ISQLBackend` implementation, replacing the default stubs provided by `psql-wire`.

## Background

The `psql-wire` library auto-detects whether an `ISQLBackend` also implements `IExtendedQueryBackend` via a type assertion in `connection.go`:

```go
if eb, ok := sqlBackend.(sqlbackend.IExtendedQueryBackend); ok {
    extBackend = eb
} else if sqlBackend != nil {
    extBackend = sqlbackend.NewDefaultExtendedQueryBackend(sqlBackend)
}
```

If the backend does not implement `IExtendedQueryBackend`, a `DefaultExtendedQueryBackend` wraps it. This default delegates `HandleExecute` to `HandleSimpleQuery` and stubs out everything else. Client libraries like pgx can connect and run unparameterised queries through this path.

No factory or wiring changes are needed to opt in. Adding the methods to the existing struct is sufficient.

## The IExtendedQueryBackend interface

```go
type IExtendedQueryBackend interface {
    HandleParse(ctx context.Context, stmtName string, query string, paramOIDs []uint32) ([]uint32, error)
    HandleBind(ctx context.Context, portalName string, stmtName string, paramFormats []int16, paramValues [][]byte, resultFormats []int16) error
    HandleDescribeStatement(ctx context.Context, stmtName string, query string, paramOIDs []uint32) ([]uint32, []sqldata.ISQLColumn, error)
    HandleDescribePortal(ctx context.Context, portalName string, stmtName string, query string, paramOIDs []uint32) ([]sqldata.ISQLColumn, error)
    HandleExecute(ctx context.Context, portalName string, stmtName string, query string, paramFormats []int16, paramValues [][]byte, resultFormats []int16, maxRows int32) (sqldata.ISQLResultStream, error)
    HandleCloseStatement(ctx context.Context, stmtName string) error
    HandleClosePortal(ctx context.Context, portalName string) error
}
```

## Migration steps

### Step 1: Add no-op stubs to the existing backend

Locate the struct in stackql that implements `ISQLBackend` (the one with `HandleSimpleQuery`, `SplitCompoundQuery`, and `GetDebugStr`). Add the seven methods below. These are direct copies of the `DefaultExtendedQueryBackend` behaviour, so robot tests should produce identical results.

```go
func (sb *YourBackend) HandleParse(ctx context.Context, stmtName string, query string, paramOIDs []uint32) ([]uint32, error) {
    return paramOIDs, nil
}

func (sb *YourBackend) HandleBind(ctx context.Context, portalName string, stmtName string, paramFormats []int16, paramValues [][]byte, resultFormats []int16) error {
    return nil
}

func (sb *YourBackend) HandleDescribeStatement(ctx context.Context, stmtName string, query string, paramOIDs []uint32) ([]uint32, []sqldata.ISQLColumn, error) {
    return paramOIDs, nil, nil
}

func (sb *YourBackend) HandleDescribePortal(ctx context.Context, portalName string, stmtName string, query string, paramOIDs []uint32) ([]sqldata.ISQLColumn, error) {
    return nil, nil
}

func (sb *YourBackend) HandleExecute(ctx context.Context, portalName string, stmtName string, query string, paramFormats []int16, paramValues [][]byte, resultFormats []int16, maxRows int32) (sqldata.ISQLResultStream, error) {
    return sb.HandleSimpleQuery(ctx, query)
}

func (sb *YourBackend) HandleCloseStatement(ctx context.Context, stmtName string) error {
    return nil
}

func (sb *YourBackend) HandleClosePortal(ctx context.Context, portalName string) error {
    return nil
}
```

**Verification**: run the full robot test suite. Behaviour should be unchanged.

### Step 2: Add a compile-time interface check

Near the top of the file, add:

```go
var _ sqlbackend.IExtendedQueryBackend = (*YourBackend)(nil)
```

This ensures the compiler catches missing methods if the interface changes.

### Step 3: Implement HandleExecute with parameter substitution

`HandleExecute` receives the query string and bound parameter values. The parameters arrive as:

- `paramFormats []int16` — one entry per parameter: `0` = text, `1` = binary. May be empty (all text) or length 1 (applies to all).
- `paramValues [][]byte` — raw bytes for each parameter. `nil` entry = SQL NULL.
- `resultFormats []int16` — requested result column formats (currently safe to ignore; the wire library encodes as text).

#### Option A: String interpolation (simplest)

Replace positional parameters (`$1`, `$2`, ...) with their text values, applying appropriate quoting, then delegate to `HandleSimpleQuery`:

```go
func (sb *YourBackend) HandleExecute(ctx context.Context, portalName string, stmtName string, query string, paramFormats []int16, paramValues [][]byte, resultFormats []int16, maxRows int32) (sqldata.ISQLResultStream, error) {
    resolved := substituteParams(query, paramFormats, paramValues)
    return sb.HandleSimpleQuery(ctx, resolved)
}
```

Where `substituteParams` replaces `$N` tokens with the corresponding text value. Rules:

- `paramValues[i] == nil` → substitute `NULL` (no quotes).
- Otherwise use the text representation from `paramValues[i]`. Quote string values with single quotes and escape embedded single quotes by doubling them (`'` → `''`).
- If `paramFormats` is empty or has length 1, treat all parameters as that format (usually 0 = text).

#### Option B: Native parameterisation

If the stackql execution engine supports parameterised queries, pass the values through directly. This avoids quoting issues and is more correct long-term.

**Verification**: write a test that connects with pgx, runs a parameterised query, and checks the results:

```go
rows, err := conn.Query(ctx, "SELECT $1::text, $2::int", "hello", 42)
```

### Step 4: Implement HandleDescribeStatement

Client libraries call Describe after Parse to learn the result column types before any rows arrive. This allows typed scanning (e.g., pgx allocates `int32` vs `string` targets).

Return parameter OIDs and result column metadata:

```go
func (sb *YourBackend) HandleDescribeStatement(ctx context.Context, stmtName string, query string, paramOIDs []uint32) ([]uint32, []sqldata.ISQLColumn, error) {
    columns, err := sb.planQuery(query) // derive columns from query planner / schema
    if err != nil {
        return nil, nil, err
    }
    return paramOIDs, columns, nil
}
```

Each `ISQLColumn` requires:
- `GetName()` — column name
- `GetObjectID()` — PostgreSQL type OID (e.g., `25` for text, `23` for int4, `16` for bool)
- `GetWidth()` — column width in bytes (use `-1` if variable)
- `GetTableId()`, `GetAttrNum()` — can be `0` if not applicable
- `GetTypeModifier()` — usually `-1`
- `GetFormat()` — `"text"` for text format

If the query planner cannot derive columns (e.g., for DDL), return `nil` columns — the wire library sends `NoData`, which is valid.

**Verification**: use pgx to prepare a statement and check that `FieldDescriptions()` returns the expected column metadata.

### Step 5: Implement HandleDescribePortal

Similar to `HandleDescribeStatement`, but for a bound portal. The portal already has its parameters bound, so column metadata may be more precise. In many cases this can delegate to the same logic:

```go
func (sb *YourBackend) HandleDescribePortal(ctx context.Context, portalName string, stmtName string, query string, paramOIDs []uint32) ([]sqldata.ISQLColumn, error) {
    _, columns, err := sb.HandleDescribeStatement(ctx, stmtName, query, paramOIDs)
    return columns, err
}
```

### Step 6: Implement HandleParse with type resolution

If stackql can resolve unspecified parameter types (OID = 0) from the query, do so in `HandleParse`. Otherwise, the current pass-through is fine — clients that send OID 0 will format parameters as text, which works with string interpolation.

```go
func (sb *YourBackend) HandleParse(ctx context.Context, stmtName string, query string, paramOIDs []uint32) ([]uint32, error) {
    resolved, err := sb.resolveParamTypes(query, paramOIDs)
    if err != nil {
        return nil, err
    }
    return resolved, nil
}
```

### Step 7: Implement HandleBind with validation

If stackql can validate parameter values at bind time, do so here. Errors returned from `HandleBind` are reported to the client before execution, and the connection enters error recovery mode (messages discarded until Sync). This gives the client a chance to re-bind with corrected values.

### Step 8: Implement HandleCloseStatement / HandleClosePortal

If stackql caches query plans or intermediate state, release them here. If not, the no-ops from step 1 are correct.

## Error recovery

The wire library handles error recovery automatically. If any `IExtendedQueryBackend` method returns an error:

1. An `ErrorResponse` is sent to the client.
2. All subsequent messages are discarded until the client sends `Sync`.
3. `Sync` sends `ReadyForQuery('E')` (failed transaction status).
4. The client can then retry or issue new commands.

Backend methods should return errors freely. They do not need to manage connection state.

## Testing strategy

Each step should be verified independently:

1. **Step 1 (stubs)**: full robot test suite — no regressions.
2. **Step 3 (execute)**: pgx test with parameterised `SELECT $1::text` — returns correct value.
3. **Step 4 (describe)**: pgx `Prepare` + check `FieldDescriptions()` — column names and OIDs match.
4. **Step 5 (describe portal)**: pgx query with `QueryRow` — typed scan works without explicit type hints.
5. **Step 6 (parse)**: pgx query with untyped parameters — server resolves types, client formats correctly.

For each step, a pgx integration test is the most realistic validation since pgx exercises the full Parse → Describe → Bind → Execute → Sync pipeline.

## Common OIDs for reference

| Type    | OID  | Go type       |
|---------|------|---------------|
| bool    | 16   | bool          |
| int2    | 21   | int16         |
| int4    | 23   | int32         |
| int8    | 20   | int64         |
| float4  | 700  | float32       |
| float8  | 701  | float64       |
| text    | 25   | string        |
| varchar | 1043 | string        |
| json    | 114  | string/[]byte |
| jsonb   | 3802 | string/[]byte |

These are defined in `github.com/lib/pq/oid` as `oid.T_bool`, `oid.T_int4`, etc.
