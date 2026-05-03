CREATE SCHEMA IF NOT EXISTS sales;

DROP TABLE IF EXISTS sales.orders;

CREATE TABLE sales.orders (
  order_id text PRIMARY KEY,
  customer_id text NOT NULL,
  order_ts timestamptz NOT NULL,
  status text NOT NULL,
  amount numeric(12, 2) NOT NULL CHECK (amount >= 0),
  currency char(3) NOT NULL,
  updated_at timestamptz NOT NULL
);

INSERT INTO sales.orders (
  order_id,
  customer_id,
  order_ts,
  status,
  amount,
  currency,
  updated_at
) VALUES
  ('O-1001', 'C-201', '2026-04-30T13:04:00Z', 'PLACED', 125.50, 'USD', '2026-04-30T13:04:05Z'),
  ('O-1002', 'C-202', '2026-04-30T13:08:10Z', 'CONFIRMED', 89.99, 'USD', '2026-04-30T13:08:15Z');

UPDATE sales.orders
SET status = 'SHIPPED',
    updated_at = '2026-04-30T13:20:01Z'
WHERE order_id = 'O-1001';

INSERT INTO sales.orders (
  order_id,
  customer_id,
  order_ts,
  status,
  amount,
  currency,
  updated_at
) VALUES
  ('O-1003', 'C-203', '2026-04-30T13:22:22Z', 'PLACED', 42.00, 'USD', '2026-04-30T13:22:31Z');

DELETE FROM sales.orders
WHERE order_id = 'O-1003';

SELECT * FROM sales.orders ORDER BY order_id;
