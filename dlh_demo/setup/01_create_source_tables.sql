-- =====================================================
-- DDL Script: Create Source Tables for dbt Presentation
-- =====================================================
-- This script creates Iceberg source tables in Dremio
-- for demonstrating all dbt materialization types
-- =====================================================

-- Drop tables if they exist (for clean setup)
DROP TABLE IF EXISTS nessie.raw.customers_iceberg;
DROP TABLE IF EXISTS nessie.raw.orders_iceberg;

-- =====================================================
-- Create Customers Table (Iceberg format)
-- =====================================================
CREATE TABLE nessie.raw.customers_iceberg (
    customer_id INT,
    name VARCHAR,
    email VARCHAR,
    country VARCHAR,
    signup_date DATE,
    customer_segment VARCHAR
)
PARTITION BY (country);

-- =====================================================
-- Create Orders Table (Iceberg format with date partitioning)
-- =====================================================
CREATE TABLE nessie.raw.orders_iceberg (
    order_id INT,
    customer_id INT,
    order_date DATE,
    amount DECIMAL(10, 2),
    product_category VARCHAR,
    order_status VARCHAR
)
PARTITION BY (order_date);
-- Partition by date for efficient incremental processing

-- =====================================================
-- Verify tables were created
-- =====================================================
-- Run these queries to confirm table creation:
-- SELECT * FROM INFORMATION_SCHEMA."TABLES" WHERE TABLE_NAME LIKE '%iceberg';
