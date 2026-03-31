
# AOT metadata

The postgres wire protocol supports many ahead of time (AOT) metadata echanges, eg:

- Columns and types returned by a query.
- Other aspects of extended querying per [the postgres documentation](https://www.postgresql.org/docs/16/protocol-overview.html#PROTOCOL-QUERY-CONCEPTS).


These are required for prepared statement execution and are used liberally by client libraries in various language runtimes.


## Pieces of work

### Support for full wire protocol messages

The main focus is support for extended queries; the theory being that this would result in seamless consumption of stackql by myriad client libraries.

That said, there is little reason not to fully support all messages [per the docs](https://www.postgresql.org/docs/16/protocol-message-formats.html).

It is assumed that the SQL backend interface will need to be expanded on order to achieve tihs.  We can begin with an implementation that leverages a blen of existing plus method stubs for gaps.  A word of caution, the existing logic is integral to correct function of the simple query function and so ideally changes should not break existing clients.  The regression test suite is visible in [the `stackql` core repository](https://github.com/stackql/stackql)
