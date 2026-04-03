
# Postgres protocol emulation

How `psql-wire` + stackql emulate a real PostgreSQL server for client libraries.

## What is implemented

### Connection lifecycle

Handshake, TLS upgrade, clear-text password auth, parameter exchange, and graceful termination per [protocol flow](https://www.postgresql.org/docs/16/protocol-flow.html).

### Simple query protocol

Full support. `ClientSimpleQuery` ('Q') parses, executes, and streams results with `RowDescription`, `DataRow`, `CommandComplete`, and `ReadyForQuery` per [message formats](https://www.postgresql.org/docs/16/protocol-message-formats.html). Compound queries (`;`-separated) are split and executed sequentially.

### Extended query protocol

Full message support for the [extended query cycle](https://www.postgresql.org/docs/16/protocol-overview.html#PROTOCOL-QUERY-CONCEPTS):

| Message | Direction | Status |
|---------|-----------|--------|
| Parse ('P') | client | Implemented — caches prepared statement |
| Bind ('B') | client | Implemented — binds params, creates portal |
| Describe ('D') | client | Implemented — returns ParameterDescription + RowDescription/NoData |
| Execute ('E') | client | Implemented — executes portal, streams results |
| Close ('C') | client | Implemented — closes statement or portal |
| Sync ('S') | client | Implemented — ends cycle, sends ReadyForQuery |
| Flush ('H') | client | Implemented — no-op (writes are unbuffered) |

### Error recovery

Matches PostgreSQL behaviour: after an extended query error, all messages are discarded until `Sync`, which sends `ReadyForQuery('E')`. Subsequent `Sync` returns to `ReadyForQuery('I')`.

### Text format passthrough

For `FormatCode=0` (text), string/`[]byte` values bypass `pgtype` encoding and write raw bytes directly. This preserves exact backend representation (e.g. sqlite `"t"`/`"f"` for booleans) while advertising correct OIDs in `RowDescription`.

### Result format negotiation

`resultFormats` from `Bind` are threaded through to `RowDescription` and data encoding, supporting per-column text/binary format selection per [protocol spec](https://www.postgresql.org/docs/16/protocol-message-formats.html).

## What requires stackql-side implementation

The `IExtendedQueryBackend` interface is fully wired. A `DefaultExtendedQueryBackend` delegates to `HandleSimpleQuery`, providing basic compatibility. For full fidelity, stackql implements:

- **`HandleParse`** — resolve unspecified parameter OIDs against stackql's type system.
- **`HandleDescribeStatement` / `HandleDescribePortal`** — return column metadata from the query planner so clients can allocate typed scan targets.
- **`HandleExecute`** — substitute bound parameter values into the execution pipeline.
- **`HandleBind`** — validate parameter formats/values before execution.
- **`HandleCloseStatement` / `HandleClosePortal`** — release cached plans.

See [stackql core repository](https://github.com/stackql/stackql) for the backend implementation.

## Not implemented

- **Copy protocol** — `CopyData`/`CopyDone`/`CopyFail` messages are intentionally ignored per [protocol spec](https://www.postgresql.org/docs/16/protocol-message-formats.html).
- **SASL / MD5 / SCRAM auth** — only clear-text password is supported.
- **Transaction blocks** — `ReadyForQuery` status is always `'I'` (idle) or `'E'` (error); `'T'` (in transaction) is never sent.
- **Portal suspension** — `Execute` with `maxRows > 0` does not yet suspend and resume portals via `ServerPortalSuspended`.
- **`FunctionCall` message** — deprecated in PostgreSQL, not implemented.
- **Notification / `LISTEN`/`NOTIFY`** — async notification messages are not emitted.

