# Design

## What it is for

Applications want to react to database changes: update a cache or a search
index, notify someone, keep a second system in step, drive a live UI. Postgres
can tell you about every change through logical replication, but using that
directly is awkward: one slot allows one reader, a reader that dies leaves WAL
piling up, the protocol is binary, and there is no way to say "resume from last
Tuesday" or "only orders that became PAID".

pg-feed sits in the middle. It is the one reader Postgres sees, it keeps a
bounded local log, and it offers consumers the thing they actually want: an
ordered, filterable, resumable stream over plain HTTP.

**Goals**

- One small, static binary; one config file; one state file.
- Postgres sees exactly one consumer.
- Never lose a change; deliver in commit order.
- Any number of consumers, each with its own filters and position.
- Bounded storage with predictable retention.

**Non-goals**

- Initial snapshots of tables (consumers do that themselves, see
  [Writing consumers](consumers.md#initial-load-then-follow)).
- DDL events, transformations, routing to other systems. It is a feed, not a
  pipeline.
- Exactly-once delivery. It is at-least-once; consumers are idempotent.
- More than one database per instance.

## How it works

```
Postgres ─ pgoutput over one slot ─▶ ingest ─▶ SQLite ─▶ readers ─▶ HTTP / SSE / WebSocket
                                       │          ▲
                                       └─ ack ────┘   (only after the SQLite commit)
```

**Ingest** (`internal/source`). One goroutine holds a replication connection and
receives pgoutput messages. It buffers the row messages of the current
transaction and, on the commit message, writes them all in one SQLite
transaction. Only then does it acknowledge the commit's end position to
Postgres. If the process dies in between, Postgres resends the transaction on
the next start; the store ignores it because `(commit_lsn, tx_index)` is unique.
`tx_index` counts every row message of the transaction, including rows of
tables the config filters out, so the pair stays stable even if the table list
changes between runs. On a clean shutdown the last stored position is
acknowledged first, so a restart replays almost nothing. That is the whole
durability story: at-least-once into the store, exactly-once within it.

When the published tables are idle but the database is busy, the slot would
otherwise hold WAL forever. pg-feed uses the server's keepalives: between
transactions, everything the server has sent is already stored, so the keepalive
position can be acknowledged and WAL released.

**Store** (`internal/store`). One SQLite file in WAL mode with `synchronous=FULL`
(a commit here is what lets us ack Postgres, so it must survive power loss, not
just a crash). Events are rows with an autoincrement integer id, the table, the
operation, and the row images as JSON text. Indexes on `(schema, table, id)` and
`committed_at` serve the common filters and the retention sweep. Named consumer
positions and a small `meta` table live in the same file.

**Readers** (`internal/api`). Every consumer, whether pulling, on SSE or on a
WebSocket, is the same loop: query events after a position with the consumer's
filters compiled to SQL, send them, then wait on a channel the store ticks after
every commit. Backlog and live events are one code path, there are no per-client
buffers, and a slow client just reads from disk more slowly without affecting
ingest or other clients. This is why one instance can serve many consumers.

**Filters** are compiled to a `WHERE` clause: table globs become equality or
`GLOB`, operations an `IN`, row conditions `json_extract(row_json, '$.field')`
comparisons, and `changed` a `json_each` lookup. Conditions test the stored JSON
with SQLite's type rules, with a cast for numeric comparisons so that
`numeric` columns (kept as strings for precision) still order numerically.

**Retention** runs on a timer: delete by age, then by size while the file is over
the cap, 5000 rows at a time, with incremental vacuum so space returns to the
filesystem.

## Decisions

**One slot, in pg-feed.** A logical slot has one reader. Instead of fighting
that (one slot per consumer would multiply WAL retention and decoding work), the
service is that reader and all fan-out happens after the store. Postgres's load
is the same whether one or a thousand consumers are attached.

**Integer ids as positions.** Postgres LSNs identify commits, not rows, and
pgoutput's per-row positions are not monotonic across interleaved transactions.
A local sequence assigned in commit order is: it is strictly increasing, cheap
to index, and trivial for consumers to persist. It has gaps after a replay
(ignored inserts consume sequence values), which is why it is only ever used as
"greater than".

**SQLite rather than Redis Streams, Kafka or Postgres itself.** The store must
keep weeks of history on disk, answer filtered range queries, and ship inside a
single binary. SQLite does all three with no extra process. Redis keeps the
window in RAM and cannot filter by column; Kafka is a cluster; writing the log
back into Postgres doubles the write load on the database being watched.

**JSON text, not a binary row format.** Consumers get JSON anyway; storing it
as text means a filter can run in SQL and an event is served without decoding.
Numbers, booleans and JSON columns keep their types; `numeric` stays a string
so money is never rounded; timestamps are normalised to RFC 3339.

**Primary key as `key`.** pgoutput flags replica-identity columns, which for
`REPLICA IDENTITY FULL` is every column. pg-feed looks up the primary key in
`pg_index` when it first sees a table, so `key` is always the thing you would
join on, and falls back to the identity columns for keyless tables.

**Protocol version 1.** pgoutput v2 streams large in-progress transactions in
pieces; v1 sends them whole at commit. Whole transactions are what consumers
want (and what the ack discipline relies on), at the cost of holding one
transaction in memory. Postgres 12 and up all speak v1.

**net/http, no framework.** Ten routes, two of them long-lived streams, and Go's
standard router already does methods and path parameters. The API is a plain
`http.Handler`, so it can also be mounted inside another Go service (for
example `gin.WrapH(api.New(...))`).

**Secrets through `${VAR}`.** The config file is meant to be committed; the
database URL and tokens come from the environment at read time.

## Guarantees

| | |
|---|---|
| Ordering | Commit order. Within a transaction, the order Postgres logged the rows. |
| Delivery | At-least-once end to end. Exactly-once into the store. |
| Durability | An acknowledged change is on disk in the store. |
| Atomicity | A transaction is stored atomically and never partially visible. |
| Resumption | Any id within the retention window; beyond it an explicit `410`. |
| Isolation of consumers | A slow or stuck consumer does not affect ingest or other consumers. |

## Limits

- One database per instance (one slot). Run one pg-feed per database.
- A transaction is buffered in memory until its commit.
- `before` and `changed` need `REPLICA IDENTITY FULL` on the table.
- TOASTed columns that did not change are not in `row` (`unchanged` lists them).
- DDL is not captured; a dropped and recreated table is just a table.
- No cross-table or aggregate filters; conditions are per row.
- The store is one file on one disk; back it up or accept reloads.

## Compared with

| | pg-feed | Debezium | Sequin | wal-listener | Supabase Realtime | LISTEN/NOTIFY + triggers |
|---|---|---|---|---|---|---|
| Runs as | one static binary | JVM + Kafka Connect (or Debezium Server) | Elixir app + its own Postgres | Go binary | Elixir app, tied to Supabase auth | nothing extra |
| Delivers to | HTTP pull, SSE, WebSocket | Kafka, Kinesis, Pub/Sub, Redis… | webhooks, Kafka, SQS, Redis, NATS… | NATS, Kafka, RabbitMQ | WebSocket | a session's LISTEN |
| History and resume | days/GB window, by id | the broker's retention | yes | the broker's | no | none; lost if nobody listens |
| Server-side filters | table, op, columns, changed columns | SMTs | yes | table, op | table, simple column eq | whatever the trigger emits |
| Best for | apps and services that want a simple feed | large data platforms | durable delivery to many sinks | feeding a message bus | Supabase apps | low-value live hints |

## Code map

```
cmd/pg-feed/main.go      flags, wiring, retention timer, shutdown
internal/config          YAML + ${VAR}, defaults, validation
internal/source          replication connection, pgoutput decoding, setup/inspect, ack discipline
internal/store           SQLite schema, append, filtered queries, retention, consumers, wake channel
internal/api             HTTP handlers: pull, SSE, WebSocket, consumers, status, metrics, auth, CORS
compose/                 the dev stack's config and sample tables
docs/                    this documentation
```

Dependencies: `jackc/pglogrepl` and `jackc/pgx` (Postgres protocol),
`modernc.org/sqlite` (SQLite without cgo), `coder/websocket`, `yaml.v3`.
