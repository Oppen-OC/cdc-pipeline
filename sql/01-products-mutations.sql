-- Step 5 of the exercise: create the table and trigger the mutations that
-- Debezium captures from the PostgreSQL write-ahead log.

CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    price DECIMAL(10, 2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- With the default REPLICA IDENTITY, the WAL only carries the primary key for
-- UPDATE/DELETE, so the "before" block of those events would contain just `id`.
-- FULL makes PostgreSQL log the whole old row, which is what Step 6 asks to
-- inspect.
ALTER TABLE products REPLICA IDENTITY FULL;

-- op = "c" (create)
INSERT INTO products (name, description, price) VALUES ('Wireless Mouse', 'Ergonomic 2.4G mouse', 25.99);
INSERT INTO products (name, description, price) VALUES ('Mechanical Keyboard', 'RGB mechanical keyboard', 89.99);

-- op = "u" (update)
UPDATE products SET price = 22.99 WHERE name = 'Wireless Mouse';

-- op = "d" (delete)
DELETE FROM products WHERE name = 'Mechanical Keyboard';
