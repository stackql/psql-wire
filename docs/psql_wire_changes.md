# Required psql-wire library changes for extended query fidelity

These changes are needed in `github.com/stackql/psql-wire` to complete postgres-fidelity extended query support in stackql.

## 1. Text format encoding must not alter value representation

**File:** `row.go` — `Column.Write()` method (line ~82)

**Problem:** When a column has a non-text OID (e.g. `T_int8`, `T_bool`), the current path does:
```go
typed.Value.Set(src)          // pgtype.Int8.Set("100000001") — works
encoder := fc.Encoder(typed)  // TextEncoder
bb, _ := encoder(ci, nil)     // outputs "100000001" (no quotes, different width)
```

For `T_bool`, `pgtype.Bool` text-encodes as `"true"/"false"` instead of sqlite's `"t"/"f"`. For `T_int8`, formatting may differ from the string that came in.

**Fix:** For `TextFormat`, bypass `pgtype.Set()` + encoder when the source value is already a string or `[]byte`. Write the raw bytes directly with a length prefix:

```go
func (column Column) Write(ctx context.Context, writer buffer.Writer, src interface{}) error {
    if column.Format == TextFormat {
        if b, ok := asTextBytes(src); ok {
            if b == nil {
                writer.AddInt32(-1) // NULL
                return nil
            }
            writer.AddInt32(int32(len(b)))
            writer.AddBytes(b)
            return nil
        }
    }
    // existing pgtype path for binary format or non-string sources
    ...
}

func asTextBytes(src interface{}) ([]byte, bool) {
    switch v := src.(type) {
    case string:
        if strings.ToLower(v) == "null" {
            return nil, true
        }
        return []byte(v), true
    case []byte:
        return v, true
    default:
        return nil, false
    }
}
```

This preserves the exact string representation from the RDBMS while still sending the correct OID in `RowDescription`. Clients see `T_int8` in the column metadata but receive text-encoded values — which is valid postgres behaviour when `FormatCode=0` (text).

## 2. Respect `resultFormats` from Bind in RowDescription

**File:** `extended_query.go` — `writeRowDescriptionFromSQLColumns()` (line ~419)

**Problem:** Format is hardcoded to `TextFormat`:
```go
colz = append(colz, Column{
    ...
    Format: TextFormat,  // always text
})
```

**Fix:** Accept `resultFormats []int16` (from the portal's Bind message) and apply per the postgres protocol rules:

```go
func writeRowDescriptionFromSQLColumns(
    ctx context.Context, writer buffer.Writer,
    columns []sqldata.ISQLColumn, resultFormats []int16,
) error {
    var colz Columns
    for i, c := range columns {
        colz = append(colz, Column{
            Table:  c.GetTableId(),
            Name:   c.GetName(),
            AttrNo: c.GetAttrNum(),
            Oid:    oid.Oid(c.GetObjectID()),
            Width:  c.GetWidth(),
            Format: resolveResultFormat(resultFormats, i),
        })
    }
    return colz.Define(ctx, writer)
}

func resolveResultFormat(formats []int16, i int) FormatCode {
    if len(formats) == 0 {
        return TextFormat
    }
    if len(formats) == 1 {
        return FormatCode(formats[0])
    }
    if i < len(formats) {
        return FormatCode(formats[i])
    }
    return TextFormat
}
```

This requires threading `resultFormats` from the portal through to the row description writer. The portal already stores `ResultFormats` — it just needs to be passed through `handleExecute` → `writeSQLResultHeader`.

## 3. Thread resultFormats through handleExecute

**File:** `extended_query.go` — `handleExecute()` (line ~248)

The portal's `ResultFormats` need to reach the data writer so `Column.Write` uses the correct encoder (text vs binary). Currently the `dataWriter` and `writeSQLResultHeader` don't receive format information.

**Fix:** Add `resultFormats []int16` to the `dataWriter` struct or pass it through the header writing path:

```go
dw := &dataWriter{
    ctx:           ctx,
    client:        conn,
    resultFormats: portal.ResultFormats,
}
```

Then in `writeSQLResultHeader`, use `dw.resultFormats` when calling `writeRowDescriptionFromSQLColumns`.

## Summary

| Change | File | Risk | Effect |
|--------|------|------|--------|
| Text bypass for string values | `row.go` | Low — only affects text format path, preserves exact string bytes | Enables OID fidelity without changing output format |
| resultFormats in RowDescription | `extended_query.go` | Low — defaults to TextFormat when formats not specified | Clients can request binary results |
| Thread formats through execute | `extended_query.go` | Low — adds field to existing struct | Connects Bind formats to result encoding |

Change 1 is the critical blocker. Changes 2-3 are enhancements for binary result support (most clients default to text results anyway).

Once change 1 lands, stackql can enable finer OID mapping (`integer`→`T_int8`, `boolean`→`T_bool`, etc.) without breaking any existing tests.
