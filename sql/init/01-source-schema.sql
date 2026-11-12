-- Tabla de origen del pipeline: la que Debezium captura del write-ahead log de
-- PostgreSQL. Se crea sola en el primer arranque (docker-entrypoint-initdb.d).

CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    price DECIMAL(10, 2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Con la REPLICA IDENTITY por defecto, el WAL solo lleva la clave primaria en
-- UPDATE/DELETE, así que el bloque "before" de esos eventos contendría solo
-- `id`. FULL hace que PostgreSQL registre la fila anterior completa.
ALTER TABLE products REPLICA IDENTITY FULL;
