-- Mutaciones de demostración que Debezium captura del write-ahead log de
-- PostgreSQL. Los esquemas ya existen (sql/init/); ejecutar con el slot de
-- replicación `debezium` activo, o los cambios previos se pierden.

-- op = "c" (create)
INSERT INTO products (name, description, price) VALUES ('Wireless Mouse', 'Ergonomic 2.4G mouse', 25.99);
INSERT INTO products (name, description, price) VALUES ('Mechanical Keyboard', 'RGB mechanical keyboard', 89.99);

-- op = "u" (update)
UPDATE products SET price = 22.99 WHERE name = 'Wireless Mouse';

-- op = "d" (delete)
DELETE FROM products WHERE name = 'Mechanical Keyboard';
