
# Postgres wire protocol

[The `postgres` wire protocol, or frontend/backend protocol](https://www.postgresql.org/docs/current/protocol.html) supports socket-based communications.  The [wire protocol message formats](https://www.postgresql.org/docs/current/protocol-message-formats.html) define the bytes transferred.  The documentation contains a [decent description of the protocol flow](https://www.postgresql.org/docs/current/protocol-flow.html).

The bahaviour of both ends of the connection is influenced by [server configuration](https://www.postgresql.org/docs/current/runtime-config.html) and [client configuration](https://www.postgresql.org/docs/current/runtime-config-client.html).

The constants that are central to the protocol are defined [in a dedicated protocl header](https://github.com/postgres/postgres/blob/46c4c7cbc6d562d5f1b784fea9527c998c190b99/src/include/libpq/protocol.h).

## Extended query

For prepared statement style queries, the protocol has an [extended query flow](https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY).  Return AOT metadata before executing query.
Expected to cache query, potentially "check" parameters.

Sequence of frontend messages for `stackql` server that does not support extended queries:

- `PqMsg_Parse` 'P'   ASCII 80
- `PqMsg_Describe` 'D' ASCII 68
- `PqMsg_Sync` 'S'   ASCII 83
- Then, having not received what is expected, the client decides to cut its losses and bail.
- `PqMsg_Terminate` 'X' ASCII 88

### Notice messages

Postgres synchronous notice messages are FYI messages that the server does **not** consider errors.  In line with this, [default behaviour for `libpq` is simply to print them to `stderr` and that is it](https://www.postgresql.org/docs/current/libpq-notice-processing.html).  Other client libraries tend to ignore them altogether (eg: [`rust` `postgres` client library](https://docs.rs/postgres/latest/postgres/enum.SimpleQueryMessage.html)); therefore one can say that these messages are not semantically significant.

[`libpq` notice handling](https://www.postgresql.org/docs/current/libpq-notice-processing.html).
[Botice DTO](https://www.postgresql.org/docs/current/libpq-exec.html#LIBPQ-PQRESULTERRORMESSAGE).
[Notice DTO fields](https://www.postgresql.org/docs/current/libpq-exec.html#LIBPQ-PQRESULTERRORFIELD).


### Rust notices specific info

- [`rust` `libpq` `PQnoticeReceiver` interface](https://docs.rs/libpq-sys/latest/libpq_sys/type.PQnoticeReceiver.html).



