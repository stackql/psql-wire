
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
