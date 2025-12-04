/*
    Model: stage_4_reflection
    Materialization: MATERIALIZED VIEW (Dremio Reflection)
    
    Description:
    This is a Dremio Reflection - a materialized view with automatic refresh.
    Reflections are Dremio's query acceleration feature that maintains
    pre-aggregated data and automatically refreshes based on policy.
    
    In Dremio 26, Reflections use Iceberg format and support:
    - Automatic incremental refresh
    - Query rewrite optimization
    - Autonomous reflection recommendations
    
    Note: Materialized view support may vary by dbt-dremio adapter version.
    If not supported, this can be created as a table with similar aggregations.
    
    Use Case:
    - Dashboard queries
    - Frequently accessed aggregations
    - Query performance optimization
    - BI tool acceleration
*/

{{ config(
    materialized='view',
    database='nessie',
    schema='dlh_demo_presentation',
    reflection=[
      {
        'type': 'aggregate',
        'name': 'stage_4_agg',
        'dimensions': ['order_date', 'order_year', 'order_month', 'country', 'customer_segment', 'customer_tier'],
        'measures': ['unique_customers', 'total_orders', 'total_revenue', 'avg_order_value', 'completed_orders', 'pending_orders', 'high_value_revenue', 'medium_value_revenue', 'low_value_revenue', 'first_time_orders', 'distinct_categories', 'avg_customer_lifetime_value', 'revenue_per_customer']
      }
    ],
    tags=['presentation', 'stage_4', 'reflection']
) }}

SELECT
    -- Date dimensions
    order_date,
    EXTRACT(YEAR FROM order_date) AS order_year,
    EXTRACT(MONTH FROM order_date) AS order_month,
    
    -- Geographic dimensions
    country,
    
    -- Customer dimensions
    customer_segment,
    customer_tier,
    
    -- Aggregated metrics
    COUNT(DISTINCT customer_id) AS unique_customers,
    COUNT(order_id) AS total_orders,
    SUM(amount) AS total_revenue,
    AVG(amount) AS avg_order_value,
    
    -- Order status metrics
    SUM(CASE WHEN order_status = 'Completed' THEN 1 ELSE 0 END) AS completed_orders,
    SUM(CASE WHEN order_status = 'Pending' THEN 1 ELSE 0 END) AS pending_orders,
    
    -- Value tier metrics
    SUM(CASE WHEN order_value_tier = 'High Value' THEN amount ELSE 0 END) AS high_value_revenue,
    SUM(CASE WHEN order_value_tier = 'Medium Value' THEN amount ELSE 0 END) AS medium_value_revenue,
    SUM(CASE WHEN order_value_tier = 'Low Value' THEN amount ELSE 0 END) AS low_value_revenue,
    
    -- First order metrics
    SUM(CASE WHEN is_first_order THEN 1 ELSE 0 END) AS first_time_orders,
    
    -- Product category metrics
    COUNT(DISTINCT product_category) AS distinct_categories,
    
    -- Calculated KPIs
    ROUND(AVG(customer_running_total), 2) AS avg_customer_lifetime_value,
    ROUND(SUM(amount) / NULLIF(COUNT(DISTINCT customer_id), 0), 2) AS revenue_per_customer

FROM {{ ref('stage_3_incremental') }}

GROUP BY
    order_date,
    EXTRACT(YEAR FROM order_date),
    EXTRACT(MONTH FROM order_date),
    country,
    customer_segment,
    customer_tier
