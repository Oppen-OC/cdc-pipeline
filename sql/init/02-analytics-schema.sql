-- Capa de almacenamiento del pipeline: la tabla de servicio que el processor
-- materializa a partir del stream CDC y de la que lee la API.
--
-- Vive en un esquema aparte de la tabla de origen para que el lado OLTP
-- (public.products) y el analítico queden claramente separados. En un
-- despliegue real serían bases de datos o clústeres distintos; aquí un esquema
-- mantiene el proyecto en un solo contenedor de PostgreSQL.
--
-- Todo el script es idempotente, así que puede volver a ejecutarse sobre una
-- base de datos en marcha.

CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.products_enriched (
    -- Misma clave primaria que la fila de origen: el processor hace UPSERT
    -- sobre ella, así que la tabla siempre guarda el estado actual de cada
    -- producto.
    id              INTEGER PRIMARY KEY,

    -- Campos copiados de la tabla de origen.
    name            VARCHAR(255)  NOT NULL,
    description     TEXT,
    price           NUMERIC(10,2) NOT NULL,

    -- Enriquecimiento añadido por el processor.
    price_with_vat  NUMERIC(10,2) NOT NULL,
    category        VARCHAR(50),

    -- Debezium emite las columnas TIMESTAMP como microsegundos desde la época;
    -- el processor las convierte antes de escribir.
    created_at      TIMESTAMPTZ,

    -- Linaje CDC.
    source_lsn      BIGINT        NOT NULL,
    last_op         CHAR(1)       NOT NULL,
    is_deleted      BOOLEAN       NOT NULL DEFAULT FALSE,
    ingested_at     TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

COMMENT ON COLUMN analytics.products_enriched.source_lsn IS
    'Posición en el write-ahead log del evento de cambio que produjo esta fila. '
    'El processor solo aplica un evento cuyo LSN sea mayor que el almacenado, lo '
    'que hace idempotente la entrega at-least-once del stream de Redis.';

COMMENT ON COLUMN analytics.products_enriched.last_op IS
    'Código de operación de Debezium del último evento aplicado: c (create), '
    'r (lectura de snapshot), u (update) o d (delete).';

COMMENT ON COLUMN analytics.products_enriched.is_deleted IS
    'Borrado lógico (soft delete). Un DELETE en la tabla de origen marca la fila '
    'en lugar de eliminarla, de modo que la capa analítica conserva el historial.';

-- La API filtra por defecto las filas borradas; un índice parcial mantiene
-- barata esa consulta sin indexar las borradas.
CREATE INDEX IF NOT EXISTS products_enriched_active_idx
    ON analytics.products_enriched (id)
    WHERE NOT is_deleted;

-- Da soporte a la agregación de GET /stats.
CREATE INDEX IF NOT EXISTS products_enriched_category_idx
    ON analytics.products_enriched (category);
