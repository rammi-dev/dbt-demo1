-- =====================================================
-- DML Script: Insert Incremental Sample Data (Partition 6)
-- =====================================================
-- This script populates the source tables with new data
-- for the 6th partition (2024-01-06) to demonstrate incremental updates
-- =====================================================

-- Orders for 2024-01-06 (Partition 6)
INSERT INTO nessie.raw.orders_iceberg 
(order_id, customer_id, order_date, amount, product_category, order_status)
VALUES
    (1021, 10, DATE '2024-01-06', 189.99, 'Electronics', 'Completed'),
    (1022, 2, DATE '2024-01-06', 45.00, 'Books', 'Pending'),
    (1023, 3, DATE '2024-01-06', 155.00, 'Clothing', 'Completed'),
    (1024, 4, DATE '2024-01-06', 399.99, 'Electronics', 'Pending');
