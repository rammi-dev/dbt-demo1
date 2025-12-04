# Data Preparation Setup

This directory contains SQL scripts to prepare the source data for the dbt presentation project.

## Prerequisites

- Access to Dremio 26 instance
- Nessie catalog configured
- Appropriate permissions to create tables and insert data

## Setup Instructions

### Step 1: Connect to Dremio

Connect to your Dremio instance using your preferred SQL client:
- Dremio Web UI SQL Runner
- DBeaver
- JDBC/ODBC client
- dbt CLI (for testing)

### Step 2: Create Source Tables

Execute the DDL script to create Iceberg tables:

```bash
# Using Dremio SQL Runner or your SQL client
# Run the contents of: 01_create_source_tables.sql
```

This will create:
- `nessie.customers_iceberg` - Customer dimension table (partitioned by country)
- `nessie.orders_iceberg` - Order fact table (partitioned by date)

### Step 3: Insert Sample Data

Execute the DML script to populate the tables:

```bash
# Run the contents of: 02_insert_sample_data.sql
```

This will insert:
- 10 customer records across different countries
- 24 order records spanning 6 days (2024-01-01 to 2024-01-06)

### Step 4: Verify Data

Run these verification queries:

```sql
-- Check customer count
SELECT COUNT(*) as customer_count FROM nessie.customers_iceberg;
-- Expected: 10

-- Check order count
SELECT COUNT(*) as order_count FROM nessie.orders_iceberg;
-- Expected: 24

-- Check orders by date (to verify partitions)
SELECT order_date, COUNT(*) as orders_per_day 
FROM nessie.orders_iceberg 
GROUP BY order_date 
ORDER BY order_date;
-- Expected: 4 orders per day for 6 days

-- Verify partition information
SELECT country, COUNT(*) as customer_count 
FROM nessie.customers_iceberg 
GROUP BY country 
ORDER BY country;
```

## Table Schemas

### customers_iceberg

| Column | Type | Description |
|--------|------|-------------|
| customer_id | INT | Unique customer identifier |
| name | VARCHAR | Customer full name |
| email | VARCHAR | Customer email address |
| country | VARCHAR | Customer country (partition key) |
| signup_date | DATE | Date customer signed up |
| customer_segment | VARCHAR | Customer segment (Premium/Standard) |

**Partitioning**: Partitioned by `country`

### orders_iceberg

| Column | Type | Description |
|--------|------|-------------|
| order_id | INT | Unique order identifier |
| customer_id | INT | Foreign key to customers |
| order_date | DATE | Date of order (partition key) |
| amount | DECIMAL(10,2) | Order amount |
| product_category | VARCHAR | Product category |
| order_status | VARCHAR | Order status (Completed/Pending) |

**Partitioning**: Partitioned by `TRUNCATE(1, order_date)` (daily partitions)

## Next Steps

After completing the data setup:

1. Update your dbt `sources.yml` to reference these tables
2. Run the dbt models to demonstrate all materialization types
3. Use the partition macro with environment variables for incremental models

## Troubleshooting

**Issue**: Tables already exist
- **Solution**: The DDL script includes `DROP TABLE IF EXISTS` statements

**Issue**: Permission denied
- **Solution**: Ensure you have CREATE TABLE and INSERT permissions on the Nessie catalog

**Issue**: Partition errors
- **Solution**: Verify your Dremio version supports Iceberg partitioning (Dremio 26+)
