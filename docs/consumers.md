# Writing consumers

pg-feed gives you an ordered log of row changes with an integer position.
Everything else is up to the consumer. These are the patterns that work, with
complete examples.

## Three rules

1. **Delivery is at-least-once.** After a crash, pg-feed may store a transaction
   it had already stored; it deduplicates that, but *you* may also see an event
   twice if you crash between processing and saving your position. Make your
   processing idempotent: upsert by `key`, or remember the last `id` you handled
   and skip anything at or below it.
2. **Save your position after the work, not before.** Whether it is a file, a
   database row, or an ack to pg-feed, record `id` only once the effect of that
   event is durable on your side.
3. **Filter on the server.** Pass `table`, `op`, `where` and `changed` instead of
   discarding events in your code. It is less traffic and, for `/v1/events`, a
   much cheaper query.

## Pull loop

The simplest consumer. Works from any language with an HTTP client, needs no
long-lived connection, and is easy to run as a cron or a worker.

```bash
#!/usr/bin/env bash
# Processes new orders. Position kept in a file.
set -euo pipefail
FEED=http://localhost:9040; TOKEN=$PG_FEED_TOKEN
after=$(cat .position 2>/dev/null || echo 0)
while true; do
  resp=$(curl -sf -H "Authorization: Bearer $TOKEN" \
    "$FEED/v1/events?after=$after&table=public.orders&op=insert&limit=500")
  echo "$resp" | jq -c '.events[]' | while read -r ev; do
    echo "new order $(echo "$ev" | jq -r '.row.id') for $(echo "$ev" | jq -r '.row.customer')"
  done
  next=$(echo "$resp" | jq '.next')
  [ "$next" != "$after" ] && { after=$next; echo "$after" > .position; } || sleep 2
done
```

Python, keeping the position in pg-feed itself:

```python
import requests, time

FEED, TOKEN, NAME = "http://localhost:9040", "...", "billing"
s = requests.Session(); s.headers["Authorization"] = f"Bearer {TOKEN}"

while True:
    r = s.get(f"{FEED}/v1/events", params={
        "consumer": NAME, "table": "public.orders", "changed": "status", "where": "status=PAID", "limit": 200})
    if r.status_code == 410:          # fell behind retention: resync, then continue from now
        full_resync(); s.put(f"{FEED}/v1/consumers/{NAME}", json={"position": s.get(f"{FEED}/v1/status").json()["store"]["newestId"]})
        continue
    r.raise_for_status()
    body = r.json()
    for ev in body["events"]:
        invoice(ev["row"])                                   # idempotent on ev["key"]
        s.put(f"{FEED}/v1/consumers/{NAME}", json={"position": ev["id"]})
    if not body["events"]:
        time.sleep(1)
```

Saving the position after every event is the safest; saving once per batch is
fine if reprocessing a batch is cheap.

## Server-Sent Events

For browsers and anything that just wants to react. The browser's `EventSource`
reconnects on its own and resumes from the last id it saw.

```html
<script>
const es = new EventSource("https://feed.example.com/v1/stream?from=now&table=public.orders&token=" + TOKEN);
es.addEventListener("change", (e) => {
  const ev = JSON.parse(e.data);
  if (ev.op === "delete") removeRow(ev.key.id);
  else upsertRow(ev.row);
});
es.onerror = () => console.log("reconnecting…");   // automatic
</script>
```

Note: a browser `EventSource` cannot send headers, hence `?token=`. Give
browsers a token of their own and rotate it.

## WebSocket with a named consumer

The richest option: filters, resume, and acks over one connection. Any
WebSocket client works; the protocol is three JSON message types.

Node or Bun:

```js
const ws = new WebSocket("ws://localhost:9040/v1/watch?token=" + process.env.PG_FEED_TOKEN);

ws.onopen = () => ws.send(JSON.stringify({
  consumer: "search-indexer",                      // pg-feed remembers our position
  tables: ["public.products", "public.categories"],
  ops: ["insert", "update", "delete"],
}));

ws.onmessage = async ({ data }) => {
  const msg = JSON.parse(data);
  if (msg.type === "ready") return console.log("resuming after", msg.after);
  if (msg.type === "error") throw new Error(`${msg.error}: ${msg.message}`);
  const ev = msg.event;
  if (ev.op === "delete") await index.delete(ev.table, ev.key);
  else await index.upsert(ev.table, ev.row);
  ws.send(JSON.stringify({ type: "ack", id: ev.id }));   // after the upsert is durable
};

ws.onclose = () => setTimeout(connect, 1000);          // reconnect; the consumer name resumes us
```

Python with `websockets`:

```python
import asyncio, json, websockets

async def main():
    async with websockets.connect("ws://localhost:9040/v1/watch", additional_headers={"Authorization": "Bearer ..."}) as ws:
        await ws.send(json.dumps({"consumer": "notifier", "where": ["status=SHIPPED"], "changed": ["status"]}))
        async for raw in ws:
            msg = json.loads(raw)
            if msg["type"] == "event":
                notify_customer(msg["event"]["row"])
                await ws.send(json.dumps({"type": "ack", "id": msg["event"]["id"]}))

asyncio.run(main())
```

Go with `github.com/coder/websocket`:

```go
conn, _, err := websocket.Dial(ctx, "ws://localhost:9040/v1/watch", &websocket.DialOptions{
	HTTPHeader: http.Header{"Authorization": {"Bearer " + token}},
})
if err != nil { return err }
defer conn.CloseNow()
wsjson.Write(ctx, conn, map[string]any{"consumer": "audit", "ops": []string{"update", "delete"}})
for {
	var msg struct {
		Type  string          `json:"type"`
		Event json.RawMessage `json:"event"`
		Error string          `json:"error"`
	}
	if err := wsjson.Read(ctx, conn, &msg); err != nil { return err }
	switch msg.Type {
	case "event":
		var ev struct{ ID int64 `json:"id"` }
		json.Unmarshal(msg.Event, &ev)
		audit(msg.Event)
		wsjson.Write(ctx, conn, map[string]any{"type": "ack", "id": ev.ID})
	case "error":
		return fmt.Errorf("feed: %s", msg.Error)
	}
}
```

## Initial load, then follow

A change stream only has changes. To build a copy of a table you need its
current contents first, without missing anything that changes while you read.

1. Ask pg-feed where the feed is **before** you read the table:
   `GET /v1/status` → `store.newestId`, say `N`.
2. Read the table (`SELECT ...`) and load it.
3. Subscribe with `after=N`.

Changes that happened between steps 1 and 2 arrive again as events in step 3.
Because you apply events by key (upsert / delete), applying them twice is
harmless. This gives you a copy that is correct once the backlog is drained,
without locking the table.

Do the same after a `410 cursor_expired`: you have been away longer than the
retention window, so the only correct move is to reload and resume from `now`.

## React to a state change

"Tell me when an order becomes PAID" is two filters together:

```
/v1/stream?table=public.orders&changed=status&where=status=PAID
```

`changed=status` keeps only updates where `status` actually changed, and
`where=status=PAID` keeps those where the new value is PAID. Updates that touch
other columns of a paid order are not delivered. This needs
`REPLICA IDENTITY FULL` on the table; without it `changed` is never present and
the filter matches nothing. Check with `GET /v1/events?table=public.orders&op=update&limit=1`:
if the event has no `before`, set the replica identity.

## Transactions

Events that were one Postgres transaction share `xid`, arrive together and in
order. A consumer that needs transactional grouping can buffer events until the
`xid` changes. pg-feed never splits a transaction across two pull responses
unless it is larger than `limit`; set `limit` above your largest transaction, or
handle a split by `xid`.

## Things to know

- **`unchanged`**: an update to a row with a large column (text over ~2 KB,
  `jsonb`, `bytea`) that was not modified arrives without that column and lists
  it in `unchanged`. Keep the previous value, or read it from the table. Setting
  `REPLICA IDENTITY FULL` makes Postgres send it in `before` as well.
- **`numeric` is a string** (`"1200.50"`), on purpose. Parse it with a decimal
  type, not a float, when money is involved.
- **Ids have gaps.** Only "greater than" means anything. Never compute counts
  or offsets from ids.
- **Many workers, one consumer name** is not coordinated by pg-feed. Each
  worker would receive every event. Partition by filter instead (one consumer
  per table or per shard of keys), or have one reader hand work to a queue.
- **Truncate** arrives as one event per table with `row: null` and `key: {}`.
  Treat it as "delete everything you have for this table".
- **Schema changes** are not events. A new column simply appears in `row` from
  then on; a dropped column disappears. Code defensively around missing keys.
