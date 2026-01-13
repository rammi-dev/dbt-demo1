-- =====================================================
-- DML Script: Update Existing Data for SCD Type 2 Demo
-- =====================================================
-- This script updates existing records to demonstrate
-- how dbt snapshots capture historical changes (SCD Type 2)
-- 
-- After running this script, run `dbt snapshot` to see:
-- - Old versions with dbt_valid_to populated
-- - New versions with dbt_valid_to = NULL
-- =====================================================

-- =====================================================
-- ORDERS - Insert new versions with same key
-- =====================================================
-- Insert new rows with same order_id but newer order_date
-- The snapshot will detect the newer timestamp and close out old versions

-- Order 1022: New version - Pending -> Completed
INSERT INTO nessie.raw.orders_iceberg 
(order_id, customer_id, order_date, amount, product_category, order_status)
VALUES (1022, 2, DATE '2026-01-14', 45.00, 'Books', 'Completed');

-- Order 1024: New version - Pending -> Shipped
INSERT INTO nessie.raw.orders_iceberg 
(order_id, customer_id, order_date, amount, product_category, order_status)
VALUES (1024, 4, DATE '2026-01-14', 399.99, 'Electronics', 'Shipped');

-- Order 1005: New version - Price adjustment
INSERT INTO nessie.raw.orders_iceberg 
(order_id, customer_id, order_date, amount, product_category, order_status)
VALUES (1005, 5, DATE '2026-01-14', 275.00, 'Home & Garden', 'Completed');

-- Order 1001: New version - Status change
INSERT INTO nessie.raw.orders_iceberg 
(order_id, customer_id, order_date, amount, product_category, order_status)
VALUES (1001, 1, DATE '2026-01-14', 150.00, 'Electronics', 'Delivered');

-- Order 1008: New version - Amount correction
INSERT INTO nessie.raw.orders_iceberg 
(order_id, customer_id, order_date, amount, product_category, order_status)
VALUES (1008, 8, DATE '2026-01-14', 95.00, 'Books', 'Completed');

-- Order 1015: New version - Status update
INSERT INTO nessie.raw.orders_iceberg 
(order_id, customer_id, order_date, amount, product_category, order_status)
VALUES (1015, 5, DATE '2026-01-14', 180.00, 'Electronics', 'Returned');

-- =====================================================
-- CUSTOMERS - Insert new versions with same key
-- =====================================================
-- Insert new rows with same customer_id but newer signup_date
-- The snapshot will detect the newer timestamp and close out old versions

-- Customer 2: New version - Segment upgrade
INSERT INTO nessie.raw.customers_iceberg 
(customer_id, name, email, country, signup_date, customer_segment)
VALUES (2, 'Bob Smith', 'bob.smith@email.com', 'Canada', DATE '2026-01-14', 'Premium');

-- Customer 5: New version - Email update
INSERT INTO nessie.raw.customers_iceberg 
(customer_id, name, email, country, signup_date, customer_segment)
VALUES (5, 'Eve Wilson', 'eve.wilson.updated@email.com', 'Australia', DATE '2026-01-14', 'Regular');

-- Customer 8: New version - Country change
INSERT INTO nessie.raw.customers_iceberg 
(customer_id, name, email, country, signup_date, customer_segment)
VALUES (8, 'Helen Taylor', 'helen.taylor@email.com', 'Canada', DATE '2026-01-14', 'Premium');

-- Customer 1: New version - Segment downgrade
INSERT INTO nessie.raw.customers_iceberg 
(customer_id, name, email, country, signup_date, customer_segment)
VALUES (1, 'Alice Johnson', 'alice.johnson@email.com', 'USA', DATE '2026-01-14', 'Regular');

-- Customer 4: New version - Name change
INSERT INTO nessie.raw.customers_iceberg 
(customer_id, name, email, country, signup_date, customer_segment)
VALUES (4, 'David Brown Jr.', 'david.brown@email.com', 'UK', DATE '2026-01-14', 'Premium');

-- Customer 10: New version - Email and country update
INSERT INTO nessie.raw.customers_iceberg 
(customer_id, name, email, country, signup_date, customer_segment)
VALUES (10, 'Jack Anderson', 'jack.anderson.new@email.com', 'Germany', DATE '2026-01-14', 'VIP');

-- =====================================================
-- VERIFICATION QUERIES (run after dbt snapshot)
-- =====================================================
-- 
-- Check orders_snapshot for historical versions:
-- SELECT order_id, order_status, amount, dbt_valid_from, dbt_valid_to 
-- FROM nessie.dlh_demo_snapshots.orders_snapshot 
-- WHERE order_id IN (1022, 1024, 1005)
-- ORDER BY order_id, dbt_valid_from;
--
-- Check customers_snapshot for historical versions:
-- SELECT customer_id, name, customer_segment, email, country, dbt_valid_from, dbt_valid_to 
-- FROM nessie.dlh_demo_snapshots.customers_snapshot 
-- WHERE customer_id IN (2, 5, 8)
-- ORDER BY customer_id, dbt_valid_from;
-- =====================================================
