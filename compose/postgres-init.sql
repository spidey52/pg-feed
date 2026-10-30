-- Sample tables for trying the feed. REPLICA IDENTITY FULL on one of them so
-- its updates and deletes carry the old row too.
create schema if not exists "order";

create table "order".parking_logs (
  id          text primary key,
  vehicle_no  text not null,
  status      text not null,
  weight_kg   integer,
  meta        jsonb,
  created_at  timestamptz not null default now()
);
alter table "order".parking_logs replica identity full;

create table "order".orders (
  id        bigserial primary key,
  customer  text not null,
  amount    numeric(12,2) not null,
  open      boolean not null default true
);
