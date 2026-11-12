"""Procesador en streaming: stream CDC de Redis -> tabla analítica enriquecida.

Lee los eventos de cambio que Debezium Server escribe en un stream de Redis,
los limpia y enriquece, y materializa el estado actual de cada producto en
analytics.products_enriched.

La entrega es at-least-once: la transacción de PostgreSQL se confirma *antes*
del XACK de Redis, así que un fallo entre ambos provoca una reentrega en lugar
de un evento perdido. Las reentregas son inofensivas porque el UPSERT solo
aplica un evento cuya posición en el WAL (source_lsn) es más reciente que la ya
almacenada.

Ante un error de PostgreSQL se distingue por tipo. Si la conexión se pierde
(OperationalError, InterfaceError) la entrada no se confirma: el procesador
reconecta por sí mismo y vuelve a releer los pendientes. Cualquier otro error de
base de datos es determinista (p. ej. un dato que no cabe en la columna), así
que se registra y se confirma para no bloquear la lista de pendientes.

La configuración llega por variables de entorno para que el mismo fichero
funcione en el host (los valores por defecto apuntan a localhost) y dentro de
Docker Compose (el compose inyecta los nombres de servicio).
"""

import json
import logging
import os
import signal
import sys
import time
from datetime import UTC, datetime

import psycopg2
import redis

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
STREAM     = os.getenv("REDIS_STREAM", "inventory.public.products")
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

# Palabra clave -> categoría. Gana la primera coincidencia, de ahí la lista en
# lugar de un diccionario.
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

# Una sola sentencia cubre todas las operaciones. En un DELETE la fila se
# conserva y se marca en lugar de borrarse (soft delete), para que la capa
# analítica mantenga el historial. La cláusula WHERE de la rama DO UPDATE es lo
# que hace idempotente todo el pipeline: un evento reentregado, o que llega
# desordenado, trae un LSN no mayor que el almacenado y se ignora.
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
    """Termina el lote en curso y sale limpiamente con Ctrl-C / docker stop."""
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
    """Debezium emite las columnas TIMESTAMP como microsegundos desde la época."""
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return datetime.fromtimestamp(value / 1_000_000, tz=UTC)


def transform(payload):
    """Convierte el payload de un evento de cambio de Debezium en una fila de la tabla analítica.

    Devuelve None para los eventos que no llevan estado de fila (tombstones, truncates).
    """
    op = payload.get("op")
    if op not in ("c", "r", "u", "d"):
        log.warning("ignoring event with unsupported op=%s", op)
        return None

    # Un DELETE no tiene bloque `after`; en el resto de operaciones el bloque
    # `before` no aporta nada. REPLICA IDENTITY FULL es lo que completa `before`.
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
    """Aplica cada evento de cambio almacenado en una entrada del stream de Redis."""
    applied = 0
    # El sink de Redis de Debezium guarda cada evento como un único campo cuyo
    # nombre es la clave del evento serializada y cuyo valor es el payload
    # serializado, así que los nombres de campo no se pueden fijar en el código.
    for raw_value in fields.values():
        if not raw_value:
            continue  # registro tombstone
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
    """Conecta a PostgreSQL, reintentando mientras arranca el contenedor de la base de datos."""
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
    """Crea el consumer group, empezando por el principio del stream.

    MKSTREAM permite arrancar el processor antes de que Debezium haya emitido su
    primer evento; BUSYGROUP solo indica que el grupo ya existe de una ejecución
    anterior, en cuyo caso se conserva la posición almacenada.
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

    # "0" reproduce las entradas pendientes de este consumidor (entregadas pero
    # nunca confirmadas) tras un fallo; una vez drenadas, el bucle pasa a ">",
    # que entrega solo eventos nuevos.
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
            continue  # timeout del bloqueo, no hay eventos nuevos

        for _stream, entries in response:
            if cursor_id == "0" and not entries:
                # No quedan entradas pendientes: se pasa a los eventos en vivo.
                cursor_id = ">"
                continue

            for entry_id, fields in entries:
                try:
                    with conn.cursor() as cur:
                        handle_entry(cur, fields)
                    conn.commit()
                except (psycopg2.OperationalError, psycopg2.InterfaceError) as exc:
                    # Conexión perdida (p. ej. PostgreSQL reiniciado) o deadlock:
                    # transitorio, así que no se confirma. La conexión se descarta
                    # siempre (un rollback sobre una conexión rota podría lanzar)
                    # y se reconecta tras una pausa para que un error repetido no
                    # gire en un bucle cerrado. Se vuelve a "0" para releer los
                    # pendientes; el resto del lote se relee desde ahí, por eso
                    # se sale del bucle.
                    log.error("database unavailable on entry %s: %s", entry_id, exc)
                    try:
                        conn.close()
                    except psycopg2.Error:
                        pass
                    time.sleep(1)
                    conn = connect_postgres()
                    cursor_id = "0"
                    break
                except psycopg2.Error as exc:
                    # Error de datos determinista (DataError, IntegrityError...):
                    # reintentar no lo arregla, se confirma igual que un malformado.
                    conn.rollback()
                    log.error("skipping entry %s after database error: %s", entry_id, exc)
                except (KeyError, ValueError, TypeError, AttributeError) as exc:
                    # Evento malformado: se confirma, de lo contrario bloquearía
                    # la lista de pendientes para siempre.
                    conn.rollback()
                    log.error("skipping malformed entry %s: %s", entry_id, exc)

                client.xack(STREAM, GROUP, entry_id)

    conn.close()
    log.info("processor stopped")


if __name__ == "__main__":
    main()
