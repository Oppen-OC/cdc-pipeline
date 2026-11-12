"""Streaming processor: Redis CDC stream -> enriched analytics table.

Reads the change events that Debezium Server writes to a Redis stream, cleans
and enriches them, and materialises the current state of every product into
analytics.products_enriched.

Delivery is at-least-once: the PostgreSQL transaction is committed *before* the
Redis XACK, so a crash in between causes a redelivery rather than a lost event.
Redeliveries are harmless because the UPSERT only applies an event whose WAL
position (source_lsn) is newer than the one already stored.

Configuration comes from environment variables so that the same file runs both
on the host (defaults point at localhost) and inside Docker Compose (the compose
file injects the service names).
"""

import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone

import psycopg2
import redis

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
STREAM     = os.getenv("REDIS_STREAM", "tutorial.public.products")
GROUP      = os.getenv("REDIS_GROUP", "analytics-processor")
CONSUMER   = os.getenv("HOSTNAME", "consumer-local")

PG = {
    "host":     os.getenv("PGHOST", "localhost"),
    "port":     int(os.getenv("PGPORT", "5432")),
    "dbname":   os.getenv("PGDATABASE", "inventory_db"),
    "user":     os.getenv("PGUSER", "postgres"),
    "password": os.getenv("PGPASSWORD", "postgres"),
}

VAT_RATE   = float(os.getenv("VAT_RATE", "0.21"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))
BLOCK_MS   = int(os.getenv("BLOCK_MS", "5000"))

# Keyword -> category. First match wins, hence a list instead of a dict.
CATEGORY_RULES = [
    (("mouse", "keyboard", "hub", "webcam", "headset"), "Peripherals"),
    (("monitor", "display", "screen"),                  "Displays"),
    (("ssd", "hdd", "disk", "drive", "memory", "ram"),  "Storage"),
    (("cable", "adapter", "charger", "dock"),           "Accessories"),
]
DEFAULT_CATEGORY = "Uncategorized"

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)-5s %(message)s",
)
log = logging.getLogger("processor")

# A single statement covers every operation. For a DELETE the row is kept and
# flagged instead of being removed (soft delete), so the analytics layer keeps
# the history. The WHERE clause on the DO UPDATE branch is what makes the whole
# pipeline idempotent: an event that is redelivered, or that arrives out of
# order, carries an LSN that is not greater than the stored one and is ignored.
UPSERT = """
INSERT INTO analytics.products_enriched
    (id, name, description, price, price_with_vat, category,
     created_at, source_lsn, last_op, is_deleted, ingested_at)
VALUES
    (%(id)s, %(name)s, %(description)s, %(price)s, %(price_with_vat)s,
     %(category)s, %(created_at)s, %(source_lsn)s, %(last_op)s,
     %(is_deleted)s, NOW())
ON CONFLICT (id) DO UPDATE SET
    name           = EXCLUDED.name,
    description    = EXCLUDED.description,
    price          = EXCLUDED.price,
    price_with_vat = EXCLUDED.price_with_vat,
    category       = EXCLUDED.category,
    created_at     = EXCLUDED.created_at,
    source_lsn     = EXCLUDED.source_lsn,
    last_op        = EXCLUDED.last_op,
    is_deleted     = EXCLUDED.is_deleted,
    ingested_at    = NOW()
WHERE EXCLUDED.source_lsn > analytics.products_enriched.source_lsn
"""

running = True


def stop(signum, _frame):
    """Finish the current batch and exit cleanly on Ctrl-C / docker stop."""
    global running
    running = False
    log.info("signal %s received, shutting down", signum)


def categorize(name):
    lowered = (name or "").lower()
    for keywords, category in CATEGORY_RULES:
        if any(keyword in lowered for keyword in keywords):
            return category
    return DEFAULT_CATEGORY


def to_datetime(value):
    """Debezium emits TIMESTAMP columns as microseconds since the epoch."""
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return datetime.fromtimestamp(value / 1_000_000, tz=timezone.utc)


def transform(payload):
    """Turn a Debezium change event payload into a row of the analytics table.

    Returns None for events that carry no row state (tombstones, truncates).
    """
    op = payload.get("op")
    if op not in ("c", "r", "u", "d"):
        log.warning("ignoring event with unsupported op=%s", op)
        return None

    # A DELETE has no `after` block, every other operation has no useful
    # `before` block. REPLICA IDENTITY FULL is what makes `before` complete.
    row = payload.get("after") or payload.get("before")
    if not row:
        return None

    price = float(row["price"])
    return {
        "id":             row["id"],
        "name":           row["name"],
        "description":    row.get("description"),
        "price":          price,
        "price_with_vat": round(price * (1 + VAT_RATE), 2),
        "category":       categorize(row["name"]),
        "created_at":     to_datetime(row.get("created_at")),
        "source_lsn":     payload["source"]["lsn"],
        "last_op":        op,
        "is_deleted":     op == "d",
    }


def handle_entry(cur, fields):
    """Apply every change event stored in one Redis stream entry."""
    applied = 0
    # The Debezium Redis sink stores each event as a single field whose name is
    # the serialized event key and whose value is the serialized event payload,
    # so the field names cannot be hardcoded.
    for _key, raw_value in fields.items():
        if not raw_value:
            continue  # tombstone record
        payload = json.loads(raw_value).get("payload")
        if not payload:
            continue
        record = transform(payload)
        if record is None:
            continue
        cur.execute(UPSERT, record)
        applied += cur.rowcount
        log.info(
            "op=%s id=%s lsn=%s applied=%s",
            record["last_op"], record["id"], record["source_lsn"], bool(cur.rowcount),
        )
    return applied


def connect_postgres(retries=30, delay=2):
    """Connect to PostgreSQL, retrying while the database container starts."""
    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(**PG)
            log.info("connected to postgres at %s:%s", PG["host"], PG["port"])
            return conn
        except psycopg2.OperationalError as exc:
            log.warning("postgres not ready (%s/%s): %s", attempt, retries, exc)
            time.sleep(delay)
    sys.exit("could not connect to postgres")


def ensure_group(client):
    """Create the consumer group, starting at the beginning of the stream.

    MKSTREAM lets the processor start before Debezium has emitted its first
    event; BUSYGROUP simply means the group already exists from a previous run,
    in which case the stored position is kept.
    """
    try:
        client.xgroup_create(STREAM, GROUP, id="0", mkstream=True)
        log.info("consumer group '%s' created on stream '%s'", GROUP, STREAM)
    except redis.exceptions.ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise
        log.info("consumer group '%s' already exists", GROUP)


def main():
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    ensure_group(client)
    conn = connect_postgres()

    # "0" replays this consumer's pending (delivered but never acknowledged)
    # entries after a crash; once they are drained the loop switches to ">",
    # which delivers only new events.
    cursor_id = "0"
    log.info("processing stream '%s' as consumer '%s'", STREAM, CONSUMER)

    while running:
        try:
            response = client.xreadgroup(
                GROUP, CONSUMER, {STREAM: cursor_id},
                count=BATCH_SIZE, block=BLOCK_MS,
            )
        except redis.exceptions.ConnectionError as exc:
            log.warning("redis connection lost: %s", exc)
            time.sleep(2)
            continue

        if not response:
            continue  # block timeout, no new events

        for _stream, entries in response:
            if cursor_id == "0" and not entries:
                # No pending entries left: move on to live events.
                cursor_id = ">"
                continue

            for entry_id, fields in entries:
                try:
                    with conn.cursor() as cur:
                        handle_entry(cur, fields)
                    conn.commit()
                except psycopg2.Error as exc:
                    # Leave the entry unacknowledged so that it is redelivered.
                    conn.rollback()
                    log.error("database error on entry %s: %s", entry_id, exc)
                    continue
                except (KeyError, ValueError, TypeError) as exc:
                    # Malformed event: acknowledge it, otherwise it would block
                    # the pending list forever.
                    conn.rollback()
                    log.error("skipping malformed entry %s: %s", entry_id, exc)

                client.xack(STREAM, GROUP, entry_id)

    conn.close()
    log.info("processor stopped")


if __name__ == "__main__":
    main()
