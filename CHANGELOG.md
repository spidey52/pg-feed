# Changelog

## 0.1.0 (2026-10-30)

First release.

- Logical replication (pgoutput v1) from one slot into a SQLite log.
- HTTP pull (`/v1/events`), Server-Sent Events (`/v1/stream`) and WebSocket
  (`/v1/watch`) delivery with the same subscription and filter grammar.
- Filters: tables (globs), operations, row conditions (`= != > >= < <= ~`,
  `null`, dotted JSON paths), changed columns.
- Positions: `after`, `from=now`, `from_time`, named consumers with acks.
- Retention by days and by size.
- Primary-key `key`, `before` and `changed` with `REPLICA IDENTITY FULL`,
  TOAST-aware `unchanged`, RFC 3339 timestamps.
- `-check` prerequisites report, `/healthz`, `/readyz`, `/metrics`.
- Bearer-token auth, CORS, static distroless image.
