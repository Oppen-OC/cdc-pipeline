"""API REST que expone los datos procesados del pipeline.

Lee exclusivamente de analytics.products_enriched, la tabla que el processor
materializa a partir del stream CDC. La tabla transaccional de origen nunca se
toca, de modo que la capa de servicio queda desacoplada de la base de datos OLTP.

Los endpoints se declaran con `def` y no con `async def`: psycopg2 es un driver
bloqueante, así que FastAPI los ejecuta en un threadpool en lugar de bloquear el
event loop.

Ejecución en local:
    uvicorn main:app --reload --port 8000
"""

import logging
import os
from contextlib import asynccontextmanager, contextmanager
from datetime import datetime
from typing import Optional

import psycopg2
from fastapi import FastAPI, HTTPException, Query
from psycopg2.extras import RealDictCursor
from psycopg2.pool import ThreadedConnectionPool
from pydantic import BaseModel, Field

PG = {
    "host":     os.getenv("PGHOST", "localhost"),
    "port":     int(os.getenv("PGPORT", "5432")),
    "dbname":   os.getenv("PGDATABASE", "inventory_db"),
    "user":     os.getenv("PGUSER", "postgres"),
    "password": os.getenv("PGPASSWORD", "postgres"),
}

POOL_MIN = int(os.getenv("POOL_MIN", "1"))
POOL_MAX = int(os.getenv("POOL_MAX", "10"))

# Logger de uvicorn: los errores salen en el log del servidor con su mismo formato.
log = logging.getLogger("uvicorn.error")


# --------------------------------------------------------------------------- #
# Modelos de respuesta. No son imprescindibles para que los endpoints funcionen,
# pero son lo que hace que /docs publique un esquema de respuesta correcto.
# --------------------------------------------------------------------------- #

class ProductOut(BaseModel):
    id: int
    name: str
    description: Optional[str] = None
    price: float
    price_with_vat: float
    category: Optional[str] = None
    created_at: Optional[datetime] = None
    last_op: str = Field(description="Operación de Debezium: c, r, u o d")
    is_deleted: bool
    ingested_at: datetime


class CategoryStat(BaseModel):
    category: Optional[str] = None
    products: int
    avg_price: Optional[float] = None


class StatsOut(BaseModel):
    total: int
    active: int
    deleted: int
    avg_price: Optional[float] = None
    min_price: Optional[float] = None
    max_price: Optional[float] = None
    last_ingested_at: Optional[datetime] = None
    by_category: list[CategoryStat]


class HealthOut(BaseModel):
    status: str
    database: str


# --------------------------------------------------------------------------- #
# Acceso a la base de datos
# --------------------------------------------------------------------------- #

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Abre el pool de conexiones al arrancar y lo cierra al apagar.

    Una única conexión compartida corrompería el protocolo en cuanto dos
    peticiones se solapen, por eso la API mantiene un pool.
    """
    app.state.pool = ThreadedConnectionPool(minconn=POOL_MIN, maxconn=POOL_MAX, **PG)
    yield
    app.state.pool.closeall()


app = FastAPI(
    title="Products Analytics API",
    description="Capa de servicio del pipeline CDC: PostgreSQL -> Debezium -> "
                "Redis -> processor -> analytics.products_enriched -> esta API.",
    version="1.0.0",
    lifespan=lifespan,
)


@contextmanager
def get_conn():
    """Toma una conexión del pool y la devuelve siempre.

    Sin la cláusula finally, cada petición fallida filtraría una conexión y la
    API se colgaría cuando el pool se agotara.
    """
    conn = app.state.pool.getconn()
    try:
        yield conn
    finally:
        app.state.pool.putconn(conn)


def query(sql, params=None, one=False):
    """Ejecuta una consulta de solo lectura y devuelve las filas como diccionarios."""
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params or {})
            return cur.fetchone() if one else cur.fetchall()


# --------------------------------------------------------------------------- #
# Endpoints
# --------------------------------------------------------------------------- #

PRODUCT_COLUMNS = """
    id, name, description, price, price_with_vat, category,
    created_at, last_op, is_deleted, ingested_at
"""


@app.get("/health", response_model=HealthOut, tags=["ops"])
def health():
    """Comprobación de vida más una consulta real a la base de datos.

    Comprobar solo que el proceso está vivo pasaría por alto el fallo más
    probable: que PostgreSQL no sea alcanzable.
    """
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
    except psycopg2.Error as exc:
        # El detalle de psycopg2 (host, puerto, usuario) va al log, no al cliente.
        log.error("health check failed: %s", exc)
        raise HTTPException(status_code=503, detail="database unavailable")
    return {"status": "ok", "database": "reachable"}


@app.get("/products", response_model=list[ProductOut], tags=["products"])
def list_products(
    limit: int = Query(50, ge=1, le=500, description="Máximo de filas a devolver"),
    offset: int = Query(0, ge=0, description="Filas a saltar"),
    include_deleted: bool = Query(False, description="Incluir las filas borradas lógicamente (soft delete)"),
    category: Optional[str] = Query(None, description="Filtrar por categoría"),
):
    """Lista el estado actual de los productos.

    Las filas borradas lógicamente se ocultan por defecto. Los filtros
    opcionales viven dentro de la consulta en lugar de concatenarse en ella, lo
    que mantiene una única consulta preparada y no deja margen a la inyección SQL.
    """
    return query(
        f"""
        SELECT {PRODUCT_COLUMNS}
        FROM analytics.products_enriched
        WHERE (%(include_deleted)s OR NOT is_deleted)
          -- el cast es necesario para que PostgreSQL infiera el tipo del
          -- parámetro, que solo se compara contra NULL
          AND (%(category)s::text IS NULL OR category = %(category)s)
        ORDER BY id
        LIMIT %(limit)s OFFSET %(offset)s
        """,
        {
            "include_deleted": include_deleted,
            "category": category,
            "limit": limit,
            "offset": offset,
        },
    )


@app.get("/products/{product_id}", response_model=ProductOut, tags=["products"])
def get_product(product_id: int):
    """Devuelve un único producto, incluidos los borrados lógicamente."""
    row = query(
        f"""
        SELECT {PRODUCT_COLUMNS}
        FROM analytics.products_enriched
        WHERE id = %(id)s
        """,
        {"id": product_id},
        one=True,
    )
    if row is None:
        raise HTTPException(status_code=404, detail=f"product {product_id} not found")
    return row


@app.get("/stats", response_model=StatsOut, tags=["analytics"])
def stats():
    """Vista agregada de la tabla materializada.

    Se usa FILTER en lugar de CASE WHEN para que todos los contadores salgan de
    una sola pasada por la tabla.
    """
    totals = query(
        """
        SELECT
            count(*)                                           AS total,
            count(*) FILTER (WHERE NOT is_deleted)             AS active,
            count(*) FILTER (WHERE is_deleted)                 AS deleted,
            round(avg(price) FILTER (WHERE NOT is_deleted), 2) AS avg_price,
            min(price) FILTER (WHERE NOT is_deleted)           AS min_price,
            max(price) FILTER (WHERE NOT is_deleted)           AS max_price,
            max(ingested_at)                                   AS last_ingested_at
        FROM analytics.products_enriched
        """,
        one=True,
    )
    by_category = query(
        """
        SELECT category,
               count(*)              AS products,
               round(avg(price), 2)  AS avg_price
        FROM analytics.products_enriched
        WHERE NOT is_deleted
        GROUP BY category
        ORDER BY products DESC, category
        """
    )
    return {**totals, "by_category": by_category}
