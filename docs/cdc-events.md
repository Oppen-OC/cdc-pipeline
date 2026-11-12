# Captured CDC events

What Debezium emits for each change to `products`, taken from a real run. The raw
events are in [`sample-events.jsonl`](./sample-events.jsonl), one JSON document per
line; `python export-events.py` regenerates them from the Redis stream into
`cdc-output/cdc_events.json`.

Two configuration details worth noting:

* `ALTER TABLE products REPLICA IDENTITY FULL` — with the default replica
  identity PostgreSQL only writes the primary key to the WAL for UPDATE/DELETE,
  so the `before` block would contain nothing but `id`. `FULL` logs the whole
  old row.
* `debezium.source.decimal.handling.mode=double` — otherwise `price` is emitted
  as a Kafka Connect `Decimal`, i.e. base64 bytes (`"Cic="` instead of `25.99`).

## Captured events

`XLEN tutorial.public.products` returns **4** events, one per mutation:

| `op` | Operation | `before` | `after` |
| --- | --- | --- | --- |
| `c` | CREATE | `null` | `id=1, Wireless Mouse, price=25.99` |
| `c` | CREATE | `null` | `id=2, Mechanical Keyboard, price=89.99` |
| `u` | UPDATE | `id=1, price=25.99` | `id=1, price=22.99` |
| `d` | DELETE | `id=2, price=89.99` | `null` |

The UPDATE event, as stored in [`sample-events.jsonl`](./sample-events.jsonl):

```json
{
  "before": { "id": 1, "name": "Wireless Mouse", "price": 25.99, "created_at": 1786981502839123 },
  "after":  { "id": 1, "name": "Wireless Mouse", "price": 22.99, "created_at": 1786981502839123 },
  "source": {
    "version": "2.5.4.Final", "connector": "postgresql", "name": "tutorial",
    "snapshot": "false", "db": "inventory_db", "schema": "public", "table": "products",
    "txId": 732, "lsn": 26596432
  },
  "op": "u",
  "ts_ms": 1786981503208
}
```

Observations:

* Each event carries a `schema` block plus a `payload` block; `payload` holds
  `before` / `after` / `source` / `op` / `ts_ms`.
* `before` is `null` for INSERTs and `after` is `null` for DELETEs — the state
  that did not exist at that point in time.
* `source.lsn` and `source.txId` tie the event to a WAL position and a
  PostgreSQL transaction; the same LSN is what ends up in `offsets.dat`, and it
  is how Debezium resumes without replaying events after a restart.
* `ts_ms` at the top level is when Debezium processed the event, while
  `source.ts_ms` is when the change was committed in the database. The gap
  between them is the CDC latency (~360 ms here, dominated by the connector
  poll interval).
* No `r` (snapshot read) events appear: the table was created after the
  connector started, so everything came through the streaming phase.
* `tombstones.on.delete=false` is set — otherwise each DELETE would be followed
  by a second record with an empty payload, which the Redis sink would store as
  a noise entry.

---
[⬅ Back to the project README](../README.md)
