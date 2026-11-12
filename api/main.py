"""REST API exposing the processed data of the pipeline.

Reads exclusively from analytics.products_enriched, the table the processor
materialises from the CDC stream. The transactional source table is never
touched, so the serving layer stays decoupled from the OLTP database.

Endpoints are declared with `def` rather than `async def`: psycopg2 is a
blocking driver, so FastAPI runs them in a threadpool instead of stalling the
event loop.

Run locally:
    uvicorn main:app --reload --port 8000
"""

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


# --------------------------------------------------------------------------- #
# Response models. Not strictly required for the endpoints to work, but they
# are what makes /docs publish a proper response schema.
# --------------------------------------------------------------------------- #

class ProductOut(BaseModel):
    id: int
    name: str
    description: Optional[str] = None
    price: float
    price_with_vat: float
    category: Optional[str] = None
    created_at: Optional[datetime] = None
    last_op: str = Field(description="Debezium operation: c, r, u or d")
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
# Database access
# --------------------------------------------------------------------------- #

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Open the connection pool on startup and close it on shutdown.

    A single shared connection would corrupt the protocol as soon as two
    requests overlap, so the API keeps a pool instead.
    """
    app.state.pool = ThreadedConnectionPool(minconn=POOL_MIN, maxconn=POOL_MAX, **PG)
    yield
    app.state.pool.closeall()


app = FastAPI(
    title="Products Analytics API",
    description="Serving layer of the CDC pipeline: PostgreSQL -> Debezium -> "
                "Redis -> processor -> analytics.products_enriched -> this API.",
    version="1.0.0",
    lifespan=lifespan,
)


@contextmanager
def get_conn():
    """Borrow a connection from the pool and always give it back.

    Without the finally clause every failed request would leak a connection
    and the API would hang once the pool is exhausted.
    """
    conn = app.state.pool.getconn()
    try:
        yield conn
    finally:
        app.state.pool.putconn(conn)


def query(sql, params=None, one=False):
    """Run a read-only query and return dict rows."""
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
    """Liveness plus a real database round-trip.

    Checking only that the process is alive would miss the most likely
    failure, which is PostgreSQL being unreachable.
    """
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
    except psycopg2.Error as exc:
        raise HTTPException(status_code=503, detail=f"database unavailable: {exc}")
    return {"status": "ok", "database": "reachable"}


@app.get("/products", response_model=list[ProductOut], tags=["products"])
def list_products(
    limit: int = Query(50, ge=1, le=500, description="Maximum rows to return"),
    offset: int = Query(0, ge=0, description="Rows to skip"),
    include_deleted: bool = Query(False, description="Include soft-deleted rows"),
    category: Optional[str] = Query(None, description="Filter by category"),
):
    """List the current state of the products.

    Soft-deleted rows are hidden by default. The optional filters live inside
    the statement instead of being concatenated into it, which keeps a single
    prepared query and leaves no room for SQL injection.
    """
    return query(
        f"""
        SELECT {PRODUCT_COLUMNS}
        FROM analytics.products_enriched
        WHERE (%(include_deleted)s OR NOT is_deleted)
          -- the cast is needed for PostgreSQL to infer the parameter type of a
          -- value that is only ever compared against NULL
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
    """Return a single product, including soft-deleted ones."""
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
    """Aggregated view of the materialised table.

    FILTER is used instead of CASE WHEN so that every counter comes from one
    pass over the table.
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
