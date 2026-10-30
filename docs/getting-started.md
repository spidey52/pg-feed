# Getting started

This walks you from an existing Postgres database to a consumer receiving its
changes. About fifteen minutes.

## 1. Prepare Postgres

pg-feed uses logical replication. Your server needs:

| setting | value | why |
|---|---|---|
| `wal_level` | `logical` | required for logical decoding; needs a restart |
| `max_replication_slots` | at least 1 free | pg-feed uses one |
| `max_wal_senders` | at least 1 free | pg-feed uses one connection |
| `max_slot_wal_keep_size` | e.g. `10GB` | **important**: caps the WAL Postgres keeps for a stopped pg-feed. Without it a long outage fills the disk |

Self-hosted: put them in `postgresql.conf` and restart. Hosted providers have
their own switches; see [Operations](operations.md#hosted-postgres-providers).

Check with:

```sql
show wal_level;                 -- logical
show max_slot_wal_keep_size;    -- not -1
```

### A role for pg-feed

pg-feed needs `REPLICATION` and `SELECT` on the tables it streams. It does not
need to be a superuser.

```sql
create role pg_feed with login replication password 'change-me';
grant usage on schema public to pg_feed;
grant select on all tables in schema public to pg_feed;
alter default privileges in schema public grant select on tables to pg_feed;  -- future tables
```

### The publication and the slot

A *publication* says which tables are streamed. A *slot* is Postgres's bookmark
for pg-feed. Create them as a superuser or the table owner:

```sql
-- everything (needs superuser), or list what you want:
create publication pg_feed for all tables;
-- create publication pg_feed for tables in schema public;              -- Postgres 15+
-- create publication pg_feed for table public.orders, public.customers;

select pg_create_logical_replication_slot('pg_feed', 'pgoutput');
```

Or let pg-feed create both from its config by setting
`source.create_if_missing: true` (then the role needs the matching privileges).

### Old row images

By default an update event carries the new row only, and a delete only the key.
If you want `before` and `changed` on a table, tell Postgres to log the old row:

```sql
alter table public.orders replica identity full;
```

This costs extra WAL on that table. Do it where you need it.

## 2. Run pg-feed

### Docker

```bash
mkdir pg-feed && cd pg-feed
curl -O https://raw.githubusercontent.com/spidey52/pg-feed/main/config.example.yaml
mv config.example.yaml config.yaml      # edit: tables, retention, listen

docker run -d --name pg-feed --restart unless-stopped \
  -p 9040:9040 \
  -v "$PWD/config.yaml:/etc/pg-feed/config.yaml:ro" \
  -v "$PWD/data:/data" \
  -e PG_FEED_DATABASE_URL='postgresql://pg_feed:change-me@db.example.internal:5432/app' \
  -e PG_FEED_TOKENS='a-long-random-token' \
  spidey52/pg-feed:latest
```

`/data` holds the SQLite log. Keep it on a persistent volume. The image runs as
uid 65532, so on a Linux host do `sudo chown 65532 data` once.

### Binary

Download a release for your platform, or build with Go 1.26 or newer:

```bash
go install github.com/spidey52/pg-feed/cmd/pg-feed@latest
PG_FEED_DATABASE_URL='postgresql://...' PG_FEED_TOKENS='...' pg-feed -config config.yaml
```

### Check before you start

```bash
pg-feed -config config.yaml -check
```

prints what pg-feed sees in Postgres (wal_level, role privileges, publication,
slot) and a `problems` list. It changes nothing and exits non-zero if anything
would stop the service from starting.

### Does it run?

```bash
curl -s -H "Authorization: Bearer $TOKEN" localhost:9040/v1/status | jq
```

`source.connected` should be `true`. Make a change in the database and
`store.newestId` moves.

## 3. Your first consumer

Pull everything so far, then keep polling:

```bash
curl -s -H "Authorization: Bearer $TOKEN" 'localhost:9040/v1/events?limit=100'
# → {"events":[...], "next": 100}
curl -s -H "Authorization: Bearer $TOKEN" 'localhost:9040/v1/events?after=100&limit=100'
```

Or hold a connection open and get events as they commit:

```bash
curl -N -H "Authorization: Bearer $TOKEN" 'localhost:9040/v1/stream?from=now'
```

Only some tables, only inserts, only big orders:

```bash
curl -N -H "Authorization: Bearer $TOKEN" \
  'localhost:9040/v1/stream?from=now&table=public.orders&op=insert&where=amount>=1000'
```

Give the consumer a name, and pg-feed remembers where it got to:

```bash
# first run: from the beginning of what is kept
curl -s -H "Authorization: Bearer $TOKEN" 'localhost:9040/v1/events?consumer=billing'
# ...process, then save the position
curl -s -X PUT -H "Authorization: Bearer $TOKEN" -d '{"position": 100}' localhost:9040/v1/consumers/billing
# next run continues after 100
curl -s -H "Authorization: Bearer $TOKEN" 'localhost:9040/v1/events?consumer=billing'
```

From here: the [API reference](api.md) has every parameter, and
[Writing consumers](consumers.md) has complete examples and the patterns that
matter (idempotency, initial load, falling behind).
