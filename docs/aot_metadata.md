
# AOT metadata

The postgres wire protocol supports many ahead of time (AOT) metadata exchanges, eg:

- Columns and types returned by a query.
- Other aspects of extended querying per [the postgres documentation](https://www.postgresql.org/docs/16/protocol-overview.html#PROTOCOL-QUERY-CONCEPTS).


These are required for prepared statement execution and are used liberally by client libraries in various language runtimes.


## Important Aspects

### Support for full wire protocol messages

The main focus is support for extended queries; the theory being that this results in seamless consumption of stackql by myriad client libraries.

That said, there is little reason not to fully support all messages [per the docs](https://www.postgresql.org/docs/16/protocol-message-formats.html).

The SQL backend interface needs some breadth in order to achieve this.  We support a blended and opt in implementation that leverages a combination of simple query only implementation plus method stubs for extended.  A word of caution, the prior logic is integral to correct function of the simple query function and so ideally changes should not break existing clients.  The regression test suite is visible in [the `stackql` core repository](https://github.com/stackql/stackql).

### Full wire protocol support with stackql

The `psql-wire` library exposes an `IExtendedQueryBackend` interface that stackql can implement to participate in the extended query protocol.  A `DefaultExtendedQueryBackend` exists that delegates execution to the simple query path, so basic compatibility comes for free.  Richer support involves the following areas within stackql:

- **Parameter type resolution**: `HandleParse` receives parameter OIDs (which may be zero/unspecified).  stackql resolves these against its own type system and returns concrete OIDs so that clients can format bind values correctly.
- **Result column metadata**: `HandleDescribeStatement` and `HandleDescribePortal` return column names, types, and widths.  stackql derives this from its query planner or schema metadata so that clients like pgx can allocate typed scan targets before any rows arrive.
- **Parameterised execution**: `HandleExecute` receives the bound parameter values alongside the original query.  stackql substitutes these into its execution pipeline, replacing the simple string-only path.
- **Statement and portal lifecycle**: `HandleCloseStatement` and `HandleClosePortal` allow stackql to release any cached plans or intermediate state associated with a named prepared statement or portal.
- **Bind-time validation**: `HandleBind` gives stackql an opportunity to validate parameter formats and values before execution, enabling early error reporting within the extended query error-recovery model (errors before `Sync` do not tear down the connection).
