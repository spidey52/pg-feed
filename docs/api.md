# API reference

Base URL: `http://host:9040`. All `/v1` endpoints need a bearer token when
`server.tokens` is set:

```
Authorization: Bearer <token>
```

Browser `EventSource` and `WebSocket` cannot set headers, so the token may also
be passed as `?token=<token>`. Prefer the header everywhere else; query strings
end up in logs.

Responses are JSON. Errors look like:

```json
{ "error": "cursor_expired", "message": "events after 1200 were deleted by retention; oldest kept is 4810" }
```

| HTTP | `error` | when |
|---|---|---|
| 400 | `bad_request` | a parameter or the subscription is malformed |
| 401 | `unauthorized` | missing or unknown token |
| 404 | `not_found` | no such consumer |
| 410 | `cursor_expired` | the requested position is older than what retention kept |
| 503 | `not_ready` | `/readyz` while not connected to Postgres |
| 500 | `internal` | anything else; the message says what |

## The event

Every endpoint delivers the same object:

```json
{
  "id": 184233,
  "op": "update",
  "database": "app",
  "schema": "public",
  "table": "orders",
  "key": { "id": 42 },
  "row": { "id": 42, "customer": "acme", "amount": "1200.50", "status": "PAID", "paid_at": "2026-10-29T16:02:11.482Z" },
  "before": { "id": 42, "customer": "acme", "amount": "1200.50", "status": "OPEN", "paid_at": null },
  "changed": ["status", "paid_at"],
  "unchanged": ["invoice_pdf"],
  "at": "2026-10-29T16:02:11.511Z",
  "lsn": "16/B374D848",
  "xid": 991203
}
```

| field | always | meaning |
|---|---|---|
| `id` | yes | Position in the feed. Integer, strictly increasing in commit order. **Not contiguous**: gaps appear after a crash replay. Use it only as "after". |
| `op` | yes | `insert`, `update`, `delete` or `truncate` |
| `database`, `schema`, `table` | yes | Where the row lives |
| `key` | yes | The primary key columns and values. Falls back to the replica identity columns when the table has no primary key. `{}` for `truncate`. |
| `row` | yes | The row after the change. `null` for `delete` and `truncate`. |
| `before` | no | The row before the change. Present for `update` and `delete` only when the table has `REPLICA IDENTITY FULL`. |
| `changed` | no | Columns whose value differs between `before` and `row`. Only on `update`, only when `before` is present. |
| `unchanged` | no | Large (TOASTed) columns Postgres did not resend because they did not change. They are missing from `row`; take them from the previous event or the table. |
| `at` | yes | Commit time, RFC 3339 UTC |
| `lsn` | yes | Commit position in the Postgres WAL, for correlation and debugging |
| `xid` | yes | Postgres transaction id. Events with the same `xid` were one transaction. |

### How column values appear

| Postgres type | JSON |
|---|---|
| `boolean` | `true` / `false` |
| `smallint`, `integer`, `bigint`, `oid` | number |
| `real`, `double precision` | number (`NaN`, `Infinity` stay strings) |
| `json`, `jsonb` | the JSON value itself |
| `timestamptz` | string, RFC 3339 in UTC: `"2026-10-29T16:02:11.482Z"` |
| `timestamp` | string, ISO without zone: `"2026-10-29T16:02:11.482"` |
| `numeric`, `decimal`, `money` | **string** (`"1200.50"`), so no precision is lost |
| `uuid`, `text`, `date`, `time`, `interval`, enums, arrays, `bytea` (`\x...`), everything else | string, exactly as Postgres prints it |
| `NULL` | `null` |

## Choosing events: the subscription

Pull and SSE take the subscription as query parameters; WebSocket takes it as a
JSON object in the first message. Same fields, same meaning.

### Where to start

| query param | JSON | meaning |
|---|---|---|
| `after=<id>` | `"after": 123` | everything after this id |
| `from=now` | `"from": "now"` | only events that commit from now on |
| `from_time=<RFC 3339>` | `"fromTime": "..."` | everything committed at or after this time |
| `consumer=<name>` | `"consumer": "billing"` | after the saved position of this consumer (from the start if it has none) |
| *(none)* | | from the oldest event kept |

A start older than what retention kept returns `410 cursor_expired`. Passing
exactly the id of the last deleted event is still fine. This holds even when
retention has emptied the store completely.

### What to receive

| query param | JSON | meaning |
|---|---|---|
| `table=a.b,c.*` (repeatable, comma-separated) | `"tables": ["a.b", "c.*"]` | tables, with `*` and `?` wildcards on `schema.table` |
| `op=insert,update` | `"ops": ["insert", "update"]` | operations: `insert`, `update`, `delete`, `truncate` |
| `where=<condition>` (repeatable) | `"where": ["status=PAID", "amount>=100"]` | conditions on `row`; all must hold |
| `changed=status,amount` | `"changed": ["status", "amount"]` | only updates where at least one of these columns changed (needs `REPLICA IDENTITY FULL`) |

No filters means every event.

### Conditions

A condition is `field` `operator` `value`, with no spaces:

```
status=PAID          equal
status!=PAID         not equal (rows where the field is null also match)
amount>=100          greater or equal      also  >  <  <=
customer~acme*       glob: * any run, ? one character, case-sensitive
deleted_at=null      is null
deleted_at!=null     is not null
meta.gate=1          dotted path into a json / jsonb column
```

- Fields are column names, optionally followed by `.key` segments into JSON
  columns. Letters, digits and underscores only.
- Values are compared as the column's JSON type: `amount>=100` compares numbers
  if `amount` is a number. `true`/`false` match booleans. Strings compare as
  strings, which makes ISO timestamps and dates order correctly
  (`created_at>=2026-10-01`).
- `numeric` columns are strings in the event (see above), but a numeric
  *value* in a condition (`amount>=100`, `amount=1200.5`) is compared as a
  number against any stored value that looks like one, including `"1200.50"`.
  That comparison is done in double precision.
- Conditions test `row`, so they never match `delete` and `truncate` events
  (whose `row` is null), except `!=` and `=null`.
- The operator is the first one found in the string, so a value may contain
  operator characters: `note~a=b*`.

## Endpoints

### `GET /v1/events`: pull

```
GET /v1/events?after=100&limit=500&table=public.orders&op=insert,update&where=amount>=100
```

`limit` defaults to 500, maximum 5000. Returns:

```json
{ "events": [ ... ], "next": 1234 }
```

`next` is the id to pass as `after` on the following call. It equals the last
event's id, or the resolved start when nothing matched. Poll in a loop; an
empty `events` means you are caught up.

### `GET /v1/stream`: Server-Sent Events

Same parameters as pull (no `limit`). Keeps the connection open and sends:

```
id: 1234
event: change
data: {"id":1234,"op":"insert", ...}

: ping
```

A `: ping` comment goes out every 15 seconds of silence so proxies keep the
connection. Browsers' `EventSource` reconnects by itself and sends
`Last-Event-ID`; when that header is present it **overrides** `after`, `from`,
`from_time` and `consumer`, so a dropped connection resumes exactly where it
stopped with no gap and no replay. The backlog since your start position is
sent first, then live events.

### `GET /v1/watch`: WebSocket

Connect, then send the subscription as the first text message within 10 seconds:

```json
{ "consumer": "billing", "tables": ["public.orders"], "ops": ["insert", "update"], "where": ["amount>=100"] }
```

Browsers send an `Origin` header; pg-feed accepts it when it is in
`server.cors_origins` (or the list holds `*`), otherwise only same-origin pages
may connect. Clients without an `Origin` header are always allowed. pg-feed
pings every 15 seconds of silence; clients must answer (every library does).

Server messages:

```json
{ "type": "ready", "after": 1200 }                  // subscription accepted; events after 1200 follow
{ "type": "event", "event": { ... } }               // one per event, backlog first, then live
{ "type": "error", "error": "bad_request", "message": "..." }   // then the socket closes
```

Client messages:

```json
{ "type": "ack", "id": 1234 }     // save the named consumer's position (only with "consumer")
```

Acks move the position forward only; an older id is ignored. The position is
saved as soon as the ack arrives. Send one after you have durably processed the
event, not before.

### `GET /v1/status`

```json
{
  "version": "0.1.0",
  "source": {
    "connected": true, "database": "app", "slot": "pg_feed", "publication": "pg_feed",
    "storedLsn": "16/B374D848", "ackedLsn": "16/B374D848", "serverLsn": "16/B374D900", "lagBytes": 184,
    "transactionsSinceStart": 42, "lastCommitAt": "2026-10-29T16:02:11.511Z",
    "connectedAt": "2026-10-29T12:00:00Z", "reconnections": 0
  },
  "store": { "oldestId": 4810, "newestId": 184233, "events": 179423, "sizeBytes": 73400320,
             "retention": { "days": 30, "maxSizeBytes": 10737418240 } }
}
```

`lagBytes` is how far pg-feed's acknowledged position is behind the server's
WAL, as of the last keepalive. `storedLsn` is the last commit written to the
store; `ackedLsn` is what Postgres has been told, which trails by up to a second.

### Consumers

| | |
|---|---|
| `GET /v1/consumers` | `{"consumers": [{"name", "position", "updatedAt"}, ...]}` |
| `GET /v1/consumers/{name}` | one consumer, or 404 |
| `PUT /v1/consumers/{name}` with `{"position": 1234}` | create or move forward; returns the consumer |
| `DELETE /v1/consumers/{name}` | forget it (204) |

A consumer is just a saved id. Many processes may read as the same consumer;
pg-feed does not coordinate them. Positions never move backwards through the
API; delete and recreate the consumer to rewind.

### Health and metrics (no token)

| | |
|---|---|
| `GET /healthz` | `200 ok` while the process runs |
| `GET /readyz` | `200 ok` when connected to Postgres, else `503` |
| `GET /metrics` | Prometheus text format, see [Operations](operations.md#monitoring) |
