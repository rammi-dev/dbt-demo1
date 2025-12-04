/*
    Model: stage_2_table
    Materialization: TABLE (Iceberg)
    
    Description:
    This is a physical Iceberg table that stores data in Dremio.
    Tables are fully materialized and provide fast query performance.
    
    Each dbt run completely replaces the table contents (full refresh).
    In Dremio 26, all tables use Apache Iceberg format for ACID compliance
    and advanced features like time travel and schema evolution.
    
    Use Case:
    - Aggregated data
    - Stable datasets
    - When query performance is critical
    - Foundation for downstream incremental models
*/

{{ config(
    materialized='table',
    database='nessie',
    schema='dlh_demo_presentation',
    tags=['presentation', 'stage_2']
) }}

SELECT
    customer_id,
    customer_name,
    country,
    customer_segment,
    
    -- Aggregations by customer
    COUNT(order_id) AS total_orders,
    SUM(amount) AS total_revenue,
    AVG(amount) AS avg_order_value,
    MAX(amount) AS max_order_value,
    MIN(amount) AS min_order_value,
    
    -- Date aggregations
    MIN(order_date) AS first_order_date,
    MAX(order_date) AS last_order_date,
    
    -- Completed orders metrics
    SUM(is_completed) AS completed_orders,
    SUM(CASE WHEN is_completed = 0 THEN 1 ELSE 0 END) AS pending_orders,
    
    -- Value tier distribution
    SUM(CASE WHEN order_value_tier = 'High Value' THEN 1 ELSE 0 END) AS high_value_orders,
    SUM(CASE WHEN order_value_tier = 'Medium Value' THEN 1 ELSE 0 END) AS medium_value_orders,
    SUM(CASE WHEN order_value_tier = 'Low Value' THEN 1 ELSE 0 END) AS low_value_orders,
    
    -- Product category metrics
    COUNT(DISTINCT product_category) AS distinct_categories,
    
    -- Calculated metrics
    CASE 
        WHEN SUM(amount) >= 500 THEN 'VIP'
        WHEN SUM(amount) >= 200 THEN 'Regular'
        ELSE 'New'
    END AS customer_tier

FROM {{ ref('stage_1_view') }}
GROUP BY 
    customer_id,
    customer_name,
    country,
    customer_segment
