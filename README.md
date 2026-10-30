# pg-feed

**A change stream for Postgres, in one small binary.**

pg-feed holds a single logical replication slot on your database, writes every
insert, update and delete to a local SQLite log, and serves that log to any
number of consumers over HTTP pull, Server-Sent Events or WebSocket, with
server-side filters and resumable positions. Think MongoDB change streams, for
Postgres, without Kafka, Debezium or a JVM.

```
                       ┌─────────────────────────────────────────┐
 Postgres ──one slot──▶│ pg-feed   ingest ─▶ SQLite log ─▶ API   │──▶ GET  /v1/events   (pull)
 (wal_level=logical)   │           retention: N days / N GB      │──▶ GET  /v1/stream   (SSE)
                       └─────────────────────────────────────────┘──▶ WS   /v1/watch    (WebSocket)
```

- **Postgres sees one consumer.** One slot, one connection. Fan-out, filtering,
  resumption and consumer bookkeeping all happen in pg-feed.
- **Durable and ordered.** Events are stored before the slot is acknowledged,
  in commit order, one transaction at a time. At-least-once, never lost.
- **Resumable.** Every event has an integer `id`. Consumers resume from an id,
  from a time, or by name, and get a clear error when they fall behind retention.
- **Filters on the server.** By table, operation, column values and which
  columns changed, so each consumer receives only what it asked for.
- **One file to run.** A static Go binary or a 20 MB distroless image. State is
  one SQLite file. Config is one YAML file.

## Quick start

```bash
git clone https://github.com/spidey52/pg-feed && cd pg-feed
docker compose up -d --build      # Postgres 18 with logical replication + pg-feed, data in ./data

# write something
docker compose exec postgres psql -U feed -d feed -c \
  "insert into \"order\".parking_logs values ('p1','BR01AB1234','AT_GATE',null,'{\"gate\":1}')"

# read it back
curl -s -H 'Authorization: Bearer dev-token' 'localhost:9040/v1/events' | jq
```

```json
{
  "events": [{
    "id": 1,
    "op": "insert",
    "database": "feed", "schema": "order", "table": "parking_logs",
    "key": { "id": "p1" },
    "row": { "id": "p1", "vehicle_no": "BR01AB1234", "status": "AT_GATE", "weight_kg": null,
             "meta": { "gate": 1 }, "created_at": "2026-10-29T18:51:49.318069Z" },
    "at": "2026-10-29T18:51:49.323Z",
    "lsn": "0/1BB5508",
    "xid": 759
  }],
  "next": 1
}
```

Tail it live:

```bash
curl -N -H 'Authorization: Bearer dev-token' \
  'localhost:9040/v1/stream?from=now&table=order.*&op=update&changed=status'
```

To point it at your own database, see [Getting started](docs/getting-started.md).
The image is on Docker Hub as `spidey52/pg-feed` (amd64 and arm64).

## Documentation

| | |
|---|---|
| [Getting started](docs/getting-started.md) | Postgres prerequisites, running pg-feed, your first consumer |
| [Configuration](docs/configuration.md) | Every config key, with defaults |
| [API reference](docs/api.md) | Endpoints, the subscription and filter grammar, event format, errors |
| [Writing consumers](docs/consumers.md) | Patterns and examples in curl, JavaScript, Python and Go |
| [Operations](docs/operations.md) | Deploying, hosted Postgres providers, monitoring, retention, failure modes |
| [Design](docs/design.md) | How it works, guarantees, limits, and why it is built this way |

## Status

Version 0.1. The API is small and meant to stay stable; see [CHANGELOG.md](CHANGELOG.md).
Images are published to Docker Hub by hand for now; the CI and release
workflows are parked under `.github/workflows.disabled`.
Tested against Postgres 18; anything from 12 up should work (pgoutput protocol 1).

## License

[MIT](LICENSE)
