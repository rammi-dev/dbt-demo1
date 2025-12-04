/*
    Model: base_data
    Materialization: EPHEMERAL
    
    Description:
    This is an ephemeral model that exists only as a CTE (Common Table Expression).
    It is not persisted in Dremio but serves as the foundation for downstream models.
    
    Ephemeral models are compiled inline into dependent models, reducing storage
    but requiring recomputation each time they are referenced.
    
    Use Case:
    - Lightweight transformations
    - Intermediate calculations that don't need persistence
    - Reducing clutter in the data warehouse
*/

{{ config(
    materialized='ephemeral',
    database='nessie',
    schema='dlh_demo_presentation',
    tags=['presentation', 'base']
) }}

-- Simplified to reference customer_orders table (avoids Nessie version context issues with views)
SELECT
    customer_id,
    name AS customer_name,
    '' AS email,
    country,
    CURRENT_DATE AS signup_date,
    'Standard' AS customer_segment,
    order_id,
    CURRENT_DATE AS order_date,
    amount,
    'General' AS product_category,
    'Completed' AS order_status,
    -- Add calculated fields
    CASE 
        WHEN amount >= 200 THEN 'High Value'
        WHEN amount >= 100 THEN 'Medium Value'
        ELSE 'Low Value'
    END AS order_value_tier
FROM {{ ref('customer_orders') }}
