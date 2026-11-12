-- Crea la tabla de origen y lanza las mutaciones que Debezium captura del
-- write-ahead log de PostgreSQL.

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

-- op = "c" (create)
INSERT INTO products (name, description, price) VALUES ('Wireless Mouse', 'Ergonomic 2.4G mouse', 25.99);
INSERT INTO products (name, description, price) VALUES ('Mechanical Keyboard', 'RGB mechanical keyboard', 89.99);

-- op = "u" (update)
UPDATE products SET price = 22.99 WHERE name = 'Wireless Mouse';

-- op = "d" (delete)
DELETE FROM products WHERE name = 'Mechanical Keyboard';
