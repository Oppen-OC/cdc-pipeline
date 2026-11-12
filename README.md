# Exercise 2 — End-to-End Data Flow Pipeline (solution)

A row inserted into a transactional PostgreSQL table is captured from the
write-ahead log, streamed through Redis, enriched by a streaming processor,
materialised into an analytical table and served over a REST API — in a few
seconds, without the writing application knowing any of it exists.

```
PostgreSQL          Debezium          Redis           processor        PostgreSQL        FastAPI
public.products ──► Server 2.5 ──► stream buffer ──► enrich +  ──► analytics.      ──► REST :8000
   (WAL)            (CDC)                             UPSERT       products_enriched
```

Full component breakdown, data flow and rationale: **[docs/architecture.md](./docs/architecture.md)**.

## Stack

| Layer | Choice |
| --- | --- |
| Ingestion | Debezium Server 2.5.4 — log-based CDC, no Kafka cluster |
| Transport | Redis 7 streams with consumer groups |
| Processing | Python 3.11, real-time streaming consumer |
| Storage | PostgreSQL 15, `analytics` schema |
| Exposure | FastAPI 0.109 + uvicorn, REST/JSON |
| Deployment | Docker Compose, five services |

## Layout

```
.
├── docker-compose.yml                    # the whole stack
├── debezium-conf/application.properties  # CDC source and sink configuration
├── sql/
│   ├── 01-products-mutations.sql         # source table + sample mutations
│   └── 02-analytics-schema.sql           # serving table (idempotent)
├── processor/
│   ├── consumer.py                       # Redis stream -> analytics table
│   ├── Dockerfile
│   └── requirements.txt
├── api/
│   ├── main.py                           # REST API over the serving table
│   ├── Dockerfile
│   └── requirements.txt
├── docs/
│   ├── architecture.md                   # architecture diagram and decisions
│   ├── cdc-events.md                     # analysis of the captured CDC events
│   └── sample-events.jsonl               # raw events from a real run
└── export-events.py                      # dumps the raw CDC events to JSON
```

## Running it

Requires Docker and Docker Compose. Nothing else — Python runs inside the
containers.

```bash
docker compose up -d --build
docker compose ps          # five services, postgres and api healthy
```

Create the schemas and generate the first change events:

```bash
docker exec -i postgres_source psql -U postgres -d inventory_db -v ON_ERROR_STOP=1 < sql/02-analytics-schema.sql
docker exec -i postgres_source psql -U postgres -d inventory_db -v ON_ERROR_STOP=1 < sql/01-products-mutations.sql
```

Watch them flow through:

```bash
docker logs -f cdc_processor
```

```
connected to postgres at postgres:5432
processing stream 'inventory.public.products' as consumer 'processor-1'
op=c id=1 lsn=26595880 applied=True
op=c id=2 lsn=26596200 applied=True
op=u id=1 lsn=26596432 applied=True
op=d id=2 lsn=26596664 applied=True
```

Teardown: `docker compose down -v`.

## API

Interactive documentation, with response schemas, at
**http://localhost:8000/docs**.

| Method | Path | Description |
| --- | --- | --- |
| GET | `/health` | Service status plus a real database round-trip; `503` if PostgreSQL is unreachable. |
| GET | `/products` | Current state of the products. Query parameters: `limit` (1-500), `offset`, `include_deleted`, `category`. |
| GET | `/products/{id}` | A single product, `404` if it does not exist. |
| GET | `/stats` | Aggregates: totals, active/deleted counts, price statistics, breakdown by category. |

```bash
curl -s localhost:8000/health
curl -s localhost:8000/products
curl -s "localhost:8000/products?include_deleted=true&limit=10"
curl -s "localhost:8000/products?category=Displays"
curl -s localhost:8000/products/1
curl -s localhost:8000/stats
```

On Windows PowerShell use `curl.exe`; the bare `curl` is an alias of
`Invoke-WebRequest` and does not accept this syntax.

## Verification

### The pipeline is live, not batch

Write to the source table and query the API. Nothing else is touched:

```bash
docker exec postgres_source psql -U postgres -d inventory_db -c \
  "INSERT INTO products (name, description, price) VALUES ('4K Monitor', '27 inch IPS panel', 329.00);"
sleep 3
curl -s localhost:8000/products
```

```
1 Wireless Mouse  22.99   27.82  Peripherals
3 USB-C Hub       45.50   55.05  Peripherals
4 4K Monitor     329.00  398.09  Displays
```

The new row arrives with VAT computed and a category derived from its name,
having crossed the WAL, Debezium, Redis and the processor on the way.

### Deletes are soft

```bash
docker exec postgres_source psql -U postgres -d inventory_db -c "DELETE FROM products WHERE id = 5;"
sleep 3
curl -s localhost:8000/products                        # gone
curl -s "localhost:8000/products?include_deleted=true" # still there, flagged
```

```
1 Wireless Mouse       active
2 Mechanical Keyboard  deleted
3 USB-C Hub            active
4 4K Monitor           active
5 4K Monitor           deleted
```

### Redelivery cannot corrupt the data

Rewind the consumer group and let the processor reprocess everything:

```bash
docker exec redis_sink redis-cli XGROUP SETID inventory.public.products analytics-processor 0
docker logs -f cdc_processor
```

```
op=c id=1 lsn=26595880 applied=False
op=c id=2 lsn=26596200 applied=False
op=u id=1 lsn=26596432 applied=False
op=d id=2 lsn=26596664 applied=False
op=c id=3 lsn=27005016 applied=False
```

Every event is rejected by the LSN guard and the `ingested_at` timestamps stay
untouched. At-least-once delivery, exactly-once effect.

### Nothing is lost while the processor is down

```bash
docker compose stop processor
docker exec postgres_source psql -U postgres -d inventory_db -c \
  "UPDATE products SET price = 39.90 WHERE id = 3;"
docker exec redis_sink redis-cli XINFO GROUPS inventory.public.products   # lag grows
docker compose start processor                                           # lag returns to 0
```

### Inspecting the raw CDC events

```bash
python export-events.py
```

Dumps the Redis stream to `cdc-output/cdc_events.json`, one change event per
line, to inspect the `op`, `before` and `after` blocks Debezium produces.

## Key design decisions

* **Redis replaces the file sink** the statement asks for — Debezium Server 2.5
  has no file sink. Redis needs no Kafka cluster and adds acknowledgements.
* **Idempotency by LSN**: each row keeps the WAL position that produced it, and
  the UPSERT ignores any event that is not strictly newer.
* **Commit before acknowledge**, so a crash causes a redelivery rather than a
  loss.
* **Soft deletes**, so the analytical layer keeps history.
* **`table.include.list`** stops the processor's own writes from being captured
  and re-processed in a loop.
* **Synchronous endpoints with a connection pool**, because psycopg2 blocks.

Each of these is explained in [docs/architecture.md](./docs/architecture.md).

## Deliverables

| # | Deliverable | Where |
| --- | --- | --- |
| 1 | Architectural diagram | [docs/architecture.md](./docs/architecture.md) |
| 2 | Code repository | This repository: `docker-compose.yml`, `processor/`, `api/`, `sql/` |
| 3 | API verification | The `curl` commands above, and http://localhost:8000/docs |
