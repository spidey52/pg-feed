# Configuration

pg-feed reads one YAML file, given with `-config path` (default `config.yaml`).
`${NAME}` anywhere in the file is replaced with the environment variable `NAME`
before parsing, so secrets stay out of the file. An unset variable becomes an
empty string. Only the `${NAME}` form is expanded; a bare `$` (in a password,
say) is left alone.

```yaml
source:
  url: ${PG_FEED_DATABASE_URL}
  slot: pg_feed
  publication: pg_feed
  create_if_missing: false
  tables: ["*"]

store:
  path: /data/pg-feed.db
  retention:
    days: 30
    max_size: 10GB
    sweep_every: 10m

server:
  listen: ":9040"
  tokens: ${PG_FEED_TOKENS}
  cors_origins: []

log:
  level: info
```

## source

| key | default | meaning |
|---|---|---|
| `url` | required | Postgres connection string (`postgresql://user:pass@host:5432/db?sslmode=require`). The role needs `REPLICATION` and `SELECT` on the streamed tables. |
| `slot` | `pg_feed` | Logical replication slot name. One slot per pg-feed instance; never share. |
| `publication` | `pg_feed` | Publication to stream. Decides which tables Postgres sends at all. |
| `create_if_missing` | `false` | Create the slot and the publication (from `tables`) when they do not exist. Off by default so a wrong `url` does nothing. |
| `tables` | `["*"]` | Which tables to keep. Also the table list used to create the publication. |

`tables` entries are `schema.table`, `schema.*` (every table in a schema) or `*`
(everything). `*` and `?` work as wildcards in either part, e.g. `public.order_*`.
When the publication sends more tables than this list, the rest are dropped at
ingest. When it sends fewer, nothing can add them: a publication is the outer limit.

With `create_if_missing: true`, `*` becomes `for all tables` (needs superuser),
`schema.*` becomes `for tables in schema` (Postgres 15+), and `schema.table`
becomes `for table`. Partial wildcards such as `public.order_*` cannot name a
publication's tables, so they are rejected together with `create_if_missing`.

## store

| key | default | meaning |
|---|---|---|
| `path` | `./data/pg-feed.db` | The SQLite file. Its directory is created. Put it on a persistent disk. |
| `retention.days` | `30` | Delete events older than this. `0` disables the age limit. |
| `retention.max_size` | none | Delete the oldest events while the file is bigger than this. Accepts `500MB`, `10GB`, `1.5TB` or plain bytes. |
| `retention.sweep_every` | `10m` | How often retention runs. Go duration: `30s`, `10m`, `1h`. |

Both limits apply; whichever is hit first wins. See
[Operations](operations.md#retention-and-disk) for sizing.

## server

| key | default | meaning |
|---|---|---|
| `listen` | `:9040` | Address to listen on. `127.0.0.1:9040` to keep it local. |
| `tokens` | empty | Comma-separated bearer tokens. Any one of them grants access to every `/v1` endpoint. **Empty means no authentication**; pg-feed logs a warning. |
| `cors_origins` | `[]` | Origins allowed to call the API from a browser, e.g. `["https://app.example.com"]`. Sets the CORS headers and decides which `Origin` values may open `/v1/watch`. `["*"]` allows all. Empty: no CORS headers, WebSocket only from the same origin. |

`/healthz`, `/readyz` and `/metrics` never require a token.

pg-feed speaks plain HTTP. For TLS put it behind a reverse proxy
([example](operations.md#tls)).

## log

| key | default | meaning |
|---|---|---|
| `level` | `info` | `debug`, `info`, `warn` or `error`. One JSON object per line on stdout. |

## Command line

```
pg-feed -config config.yaml     run
pg-feed -config config.yaml -check
                                connect to Postgres, print a report of settings, role,
                                publication and slot, list problems, exit 1 if any
pg-feed -version
```

## Environment variables

pg-feed itself reads no environment variables. The config file does, through
`${NAME}`. The names used in the examples are just conventions:

| variable | used for |
|---|---|
| `PG_FEED_DATABASE_URL` | `source.url` |
| `PG_FEED_TOKENS` | `server.tokens` |
