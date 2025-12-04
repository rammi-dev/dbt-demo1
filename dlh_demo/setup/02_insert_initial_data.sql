-- =====================================================
-- DML Script: Insert Initial Sample Data (Partitions 1-5)
-- =====================================================
-- This script populates the source tables with initial data
-- for the first 5 partitions (2024-01-01 to 2024-01-05)
-- =====================================================

-- =====================================================
-- Insert Customers Data
-- =====================================================
INSERT INTO nessie.raw.customers_iceberg 
(customer_id, name, email, country, signup_date, customer_segment)
VALUES
    (1, 'Alice Johnson', 'alice.j@email.com', 'USA', DATE '2023-01-15', 'Premium'),
    (2, 'Bob Smith', 'bob.smith@email.com', 'UK', DATE '2023-02-20', 'Standard'),
    (3, 'Carlos Garcia', 'carlos.g@email.com', 'Spain', DATE '2023-03-10', 'Premium'),
    (4, 'Diana Chen', 'diana.c@email.com', 'China', DATE '2023-04-05', 'Standard'),
    (5, 'Erik Mueller', 'erik.m@email.com', 'Germany', DATE '2023-05-12', 'Premium'),
    (6, 'Fatima Ahmed', 'fatima.a@email.com', 'UAE', DATE '2023-06-18', 'Standard'),
    (7, 'George Brown', 'george.b@email.com', 'Canada', DATE '2023-07-22', 'Premium'),
    (8, 'Hannah Lee', 'hannah.l@email.com', 'South Korea', DATE '2023-08-30', 'Standard'),
    (9, 'Ivan Petrov', 'ivan.p@email.com', 'Russia', DATE '2023-09-14', 'Premium'),
    (10, 'Julia Santos', 'julia.s@email.com', 'Brazil', DATE '2023-10-25', 'Standard');

-- =====================================================
-- Insert Orders Data (Partitions 1-5)
-- =====================================================

-- Orders for 2024-01-01 (Partition 1)
INSERT INTO nessie.raw.orders_iceberg 
(order_id, customer_id, order_date, amount, product_category, order_status)
VALUES
    (1001, 1, DATE '2024-01-01', 150.00, 'Electronics', 'Completed'),
    (1002, 2, DATE '2024-01-01', 75.50, 'Books', 'Completed'),
    (1003, 3, DATE '2024-01-01', 220.00, 'Clothing', 'Completed'),
    (1004, 4, DATE '2024-01-01', 99.99, 'Electronics', 'Completed');

-- Orders for 2024-01-02 (Partition 2)
INSERT INTO nessie.raw.orders_iceberg 
(order_id, customer_id, order_date, amount, product_category, order_status)
VALUES
    (1005, 5, DATE '2024-01-02', 450.00, 'Electronics', 'Completed'),
    (1006, 1, DATE '2024-01-02', 125.00, 'Home & Garden', 'Completed'),
    (1007, 6, DATE '2024-01-02', 89.99, 'Books', 'Completed'),
    (1008, 7, DATE '2024-01-02', 310.00, 'Clothing', 'Completed');

-- Orders for 2024-01-03 (Partition 3)
INSERT INTO nessie.raw.orders_iceberg 
(order_id, customer_id, order_date, amount, product_category, order_status)
VALUES
    (1009, 8, DATE '2024-01-03', 199.99, 'Electronics', 'Completed'),
    (1010, 2, DATE '2024-01-03', 55.00, 'Books', 'Completed'),
    (1011, 9, DATE '2024-01-03', 175.50, 'Clothing', 'Completed'),
    (1012, 3, DATE '2024-01-03', 420.00, 'Electronics', 'Completed');

-- Orders for 2024-01-04 (Partition 4)
INSERT INTO nessie.raw.orders_iceberg 
(order_id, customer_id, order_date, amount, product_category, order_status)
VALUES
    (1013, 10, DATE '2024-01-04', 88.00, 'Home & Garden', 'Completed'),
    (1014, 4, DATE '2024-01-04', 145.00, 'Electronics', 'Completed'),
    (1015, 5, DATE '2024-01-04', 210.00, 'Clothing', 'Completed'),
    (1016, 6, DATE '2024-01-04', 95.50, 'Books', 'Completed');

-- Orders for 2024-01-05 (Partition 5)
INSERT INTO nessie.raw.orders_iceberg 
(order_id, customer_id, order_date, amount, product_category, order_status)
VALUES
    (1017, 7, DATE '2024-01-05', 330.00, 'Electronics', 'Completed'),
    (1018, 8, DATE '2024-01-05', 125.00, 'Clothing', 'Completed'),
    (1019, 1, DATE '2024-01-05', 67.99, 'Books', 'Completed'),
    (1020, 9, DATE '2024-01-05', 275.00, 'Home & Garden', 'Completed');
