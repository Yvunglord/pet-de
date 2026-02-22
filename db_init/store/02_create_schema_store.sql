CREATE TABLE IF NOT EXISTS categories (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS products (
    product_id INTEGER NOT NULL,
    title TEXT NOT NULL,
    price NUMERIC NOT NULL,
    category_id INTEGER REFERENCES categories(id) ON DELETE SET NULL,
    category_name TEXT,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (product_id)
);

CREATE INDEX IF NOT EXISTS idx_products_category ON products(category_id);

COMMENT ON TABLE products IS 'Product catalog from FakeStore API';