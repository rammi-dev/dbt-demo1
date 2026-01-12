/*
    Model: stage_1_view
    Materialization: VIEW
    
    Description:
    This is a logical view that provides a virtual layer over the base data.
    Views do not store data physically - they execute the query each time accessed.
    
    In Dremio, views are lightweight and always show the most current data,
    but may have slower query performance compared to materialized options.
    
    Use Case:
    - Frequently changing data
    - Simple transformations
    - When storage is a concern
    - Security/access control layers
*/

{{ config(
    materialized='view',
    database='nessie',
    schema='dlh_demo_presentation',
    tags=['presentation', 'stage_1']
) }}

SELECT
    customer_id,
    customer_name,
    email,
    country,
    customer_segment,
    order_id,
    order_date,
    amount,
    product_category,
    order_status,
    order_value_tier,
    
    -- Add view-specific transformations
    EXTRACT(YEAR FROM order_date) AS order_year,
    EXTRACT(MONTH FROM order_date) AS order_month,
    EXTRACT(DAY FROM order_date) AS order_day,
    
    -- Calculate days since signup (as integer)
    CAST((order_date - signup_date) AS INT) AS days_since_signup,
    
    -- Flag for completed orders
    CASE WHEN order_status = 'Completed' THEN 1 ELSE 0 END AS is_completed

FROM {{ ref('base_data') }}
WHERE order_id IS NOT NULL  -- Only include customers with orders
