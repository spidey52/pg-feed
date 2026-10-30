# Operations

## Deploying

pg-feed is one process with one state file. Run **exactly one instance per
replication slot**: a second instance refuses to start while the first holds the
slot. Scale readers, not pg-feed; it serves thousands of concurrent consumers
from one process because every consumer is just a cursor over the SQLite file.

Needs: a few tens of MB of RAM, plus room for the largest single transaction
(it is held in memory until commit); negligible CPU; disk for the store (see
Retention).

### Docker

```bash
docker run -d --name pg-feed --restart unless-stopped \
  -p 9040:9040 \
  -v /srv/pg-feed/config.yaml:/etc/pg-feed/config.yaml:ro \
  -v /srv/pg-feed/data:/data \
  -e PG_FEED_DATABASE_URL='postgresql://pg_feed:...@db:5432/app?sslmode=require' \
  -e PG_FEED_TOKENS='token-for-app,token-for-browser' \
  spidey52/pg-feed:0.1.0
```

Pin a version. The image is on Docker Hub (`spidey52/pg-feed`, amd64 and
arm64), distroless, and runs as uid 65532; `chown 65532 /srv/pg-feed/data`
once on Linux hosts. Health check: `GET /readyz`.

Do not run it where instances come and go or share no disk (serverless
containers, autoscaled groups). The slot and the store are both stateful.

### systemd

```ini
# /etc/systemd/system/pg-feed.service
[Unit]
Description=pg-feed Postgres change stream
After=network-online.target

[Service]
User=pg-feed
EnvironmentFile=/etc/pg-feed/env          # PG_FEED_DATABASE_URL=..., PG_FEED_TOKENS=...
ExecStart=/usr/local/bin/pg-feed -config /etc/pg-feed/config.yaml
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
```

### Compose

The repository's `compose.yaml` runs Postgres 18 with logical replication and
pg-feed together, with data under `./data`. It is meant for development and for
trying things; for production point pg-feed at your real database.

## Hosted Postgres providers

Logical replication is a server setting, so each provider has a switch. Always
connect pg-feed **directly to the database**, not through a connection pooler
(PgBouncer, RDS Proxy, Supabase's pooler port): poolers do not carry
replication connections.

| provider | enabling logical replication | notes |
|---|---|---|
| Self-hosted | `wal_level = logical` in `postgresql.conf`, restart | also set `max_slot_wal_keep_size` |
| Google Cloud SQL | database flag `cloudsql.logical_decoding = on`, restart | grant the role `REPLICATION` with `alter user pg_feed with replication` |
| Amazon RDS / Aurora | parameter group `rds.logical_replication = 1`, reboot | grant `rds_replication` to the role |
| Azure Database for PostgreSQL (Flexible) | server parameter `wal_level = logical`, restart | `alter role pg_feed with replication` as the admin user |
| Supabase | on by default | use the direct connection (port 5432), not the pooler |
| Neon | enable logical replication in project settings | one-way, cannot be turned off |

Run `pg-feed -check` after enabling; it reports what the server actually says.

## Monitoring

`GET /metrics` serves Prometheus text format without authentication:

| metric | type | meaning |
|---|---|---|
| `pg_feed_connected` | gauge | 1 while the replication connection is up |
| `pg_feed_lag_bytes` | gauge | bytes between Postgres's WAL position and what pg-feed acknowledged |
| `pg_feed_transactions_total` | counter | transactions stored since the process started |
| `pg_feed_reconnections_total` | counter | replication reconnects since start |
| `pg_feed_last_commit_timestamp_seconds` | gauge | commit time of the last stored transaction |
| `pg_feed_store_events` | gauge | events currently kept |
| `pg_feed_store_size_bytes` | gauge | bytes the store file uses |
| `pg_feed_store_oldest_id`, `pg_feed_store_newest_id` | gauge | the kept range |

Alerts worth having:

- `pg_feed_connected == 0` for more than a minute.
- `pg_feed_lag_bytes` growing for several minutes: pg-feed cannot keep up, or
  the store disk is slow or full.
- `pg_feed_store_size_bytes` near `max_size`: retention is cutting history
  shorter than `days`; consumers that pause may hit `cursor_expired`.
- On the Postgres side, WAL retained by the slot, which grows whenever pg-feed
  is down:

```sql
select slot_name, active, wal_status,
       pg_size_pretty(pg_current_wal_lsn() - confirmed_flush_lsn) as retained
from pg_replication_slots where slot_name = 'pg_feed';
```

`wal_status = lost` means `max_slot_wal_keep_size` was exceeded; see Failure
modes.

Logs are one JSON object per line on stdout (`level`, `msg`, fields). Startup
logs `listening` and `replication started`; every reconnect logs an error with
the cause.

## Retention and disk

Events are deleted when older than `retention.days` **or** when the file exceeds
`retention.max_size`, whichever comes first, every `sweep_every`. Freed pages are
returned to the filesystem incrementally.

Sizing: an event is roughly the row as JSON plus 200 bytes, twice that with
`REPLICA IDENTITY FULL` (the `before` row). As a rule of thumb:

| row changes per day | ~1 KB rows, 30 days |
|---|---|
| 100 000 | 3 GB |
| 1 000 000 | 30 GB |
| 10 000 000 | 300 GB |

Keep `max_size` well under the disk, and `days` as long as the longest outage a
consumer may have. A consumer that resumes from before the oldest kept event gets
`410 cursor_expired` and must reload (see [Writing consumers](consumers.md#initial-load-then-follow)).

The store is a single SQLite file in WAL mode. Back it up online with:

```bash
sqlite3 /data/pg-feed.db ".backup /backup/pg-feed-$(date +%F).db"
```

Losing the store loses history, not correctness: pg-feed continues from the
slot's position, but consumers that were behind must reload.

## Failure modes

| what happens | effect | what to do |
|---|---|---|
| Postgres restarts or the network blips | pg-feed reconnects every 5 s; nothing is lost, the slot holds the position | nothing |
| pg-feed is down | Postgres retains WAL from the slot's position, up to `max_slot_wal_keep_size`; on restart pg-feed catches up | watch retained WAL; size `max_slot_wal_keep_size` for your longest acceptable outage |
| pg-feed is down longer than `max_slot_wal_keep_size` allows | slot `wal_status` becomes `lost`; pg-feed cannot resume | drop and recreate the slot (`select pg_drop_replication_slot('pg_feed'); select pg_create_logical_replication_slot('pg_feed','pgoutput')`); every consumer must reload because changes were missed |
| crash between storing a transaction and acknowledging it | the transaction is sent again; the store ignores it (`commit_lsn, tx_index` is unique); ids get a gap | nothing |
| store file deleted | history gone; pg-feed starts empty from the slot's position | consumers reload |
| store file points at a different database or cluster | pg-feed refuses to start ("store was filled from ...") | use a new store file per database |
| a second instance starts | it fails: "slot is already in use" | run one instance |
| a table is added | with `for all tables` it is streamed at once; otherwise `alter publication pg_feed add table ...` | |
| a table is altered | new columns appear in `row`; nothing else | |
| a huge transaction (millions of rows) | held in memory until its commit | give pg-feed the memory, or split the batch job |
| disk full | the store cannot commit; pg-feed exits and restarts, lag grows | lower `max_size`, add disk |

Resetting everything:

```sql
select pg_drop_replication_slot('pg_feed');
drop publication pg_feed;
```

then delete the store file and start pg-feed again (with `create_if_missing` or
after recreating both).

## Security

- **Tokens.** Use long random strings. Several tokens may be listed, so give each
  consumer group its own and rotate by adding the new one before removing the old.
  With no tokens configured the API is open; pg-feed logs a warning.
- **Network.** Bind to a private interface (`listen: 10.0.0.5:9040`) or keep the
  port inside your network. The feed carries your row data.
- **TLS.** pg-feed serves plain HTTP. Terminate TLS in front of it. Caddy:

  ```
  feed.example.com {
      reverse_proxy localhost:9040 {
          flush_interval -1        # do not buffer SSE
      }
  }
  ```

  nginx needs `proxy_buffering off;` and `proxy_read_timeout 1h;` on the
  `/v1/stream` and `/v1/watch` locations, plus the usual WebSocket `Upgrade`
  headers for `/v1/watch`.
- **Browsers.** A token in `?token=` is visible in browser history and proxy
  logs. Use a dedicated token for browser clients and set `cors_origins` to your
  site rather than `*`.
- **Postgres role.** `REPLICATION` plus `SELECT` is enough. Do not run pg-feed as
  a superuser in production.

## Upgrading

Stop, replace the binary or image tag, start. Startup is instant and the slot
resumes where it left off. Within 0.x a release may change the store format; the
changelog says so, and then the store file must be deleted (consumers reload) or
migrated as described there.
