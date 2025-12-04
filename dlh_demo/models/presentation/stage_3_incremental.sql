/*
    Model: stage_3_incremental
    Materialization: INCREMENTAL (Iceberg with Partitioning)
    
    Description:
    This is an incremental Iceberg table that only processes new or changed data.
    Incremental models are ideal for large datasets where full refresh is expensive.
    
    This model demonstrates:
    - Partition filtering using environment variables (via get_partition_filter macro)
    - Incremental logic with is_incremental() check
    - Merge strategy for updating existing records
    - Iceberg partitioning for efficient incremental refresh
    
    Environment Variable Usage:
    export DBT_PARTITION_DATE='2024-01-02'
    dbt run --select stage_3_incremental
    
    Use Case:
    - Large fact tables
    - Event/log data
    - Time-series data
    - When full refresh is too slow/expensive
*/

{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='merge',
    partition_by='order_date',
    database='nessie',
    schema='dlh_demo_presentation',
    tags=['presentation', 'stage_3', 'incremental']
) }}

WITH daily_order_summary AS (
    SELECT
        v.order_id,
        v.order_date,
        v.customer_id,
        v.customer_name,
        v.country,
        v.customer_segment,
        v.amount,
        v.product_category,
        v.order_status,
        v.order_value_tier,
        
        -- Join with customer aggregations from stage_2
        t.total_orders AS customer_total_orders,
        t.total_revenue AS customer_total_revenue,
        t.customer_tier,
        
        -- Calculate order rank for this customer
        ROW_NUMBER() OVER (PARTITION BY v.customer_id ORDER BY v.order_date) AS order_sequence,
        
        -- Running total for customer
        SUM(v.amount) OVER (
            PARTITION BY v.customer_id 
            ORDER BY v.order_date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS customer_running_total
        
    FROM {{ ref('stage_1_view') }} v
    LEFT JOIN {{ ref('stage_2_table') }} t
        ON v.customer_id = t.customer_id
    
    {% if is_incremental() %}
        -- Incremental filter using partition macro
        WHERE {{ get_partition_filter(partition_column='order_date', start_date_var='DBT_PARTITION_DATE', default_days_back=7, use_range=false) }}
    {% endif %}
)

SELECT
    order_id,
    order_date,
    customer_id,
    customer_name,
    country,
    customer_segment,
    amount,
    product_category,
    order_status,
    order_value_tier,
    customer_total_orders,
    customer_total_revenue,
    customer_tier,
    order_sequence,
    customer_running_total,
    
    -- Add incremental processing metadata
    CURRENT_TIMESTAMP AS last_updated_at,
    
    -- Flag for first order
    CASE WHEN order_sequence = 1 THEN TRUE ELSE FALSE END AS is_first_order

FROM daily_order_summary
