/*
    Snapshot: orders_snapshot
    Strategy: CHECK
    
    Description:
    This snapshot tracks slowly changing dimensions (SCD Type 2) for order data.
    It captures changes to order status and amount using the CHECK strategy.
    
    The CHECK strategy compares specified columns to detect changes, making it ideal
    when you don't have an updated_at timestamp column.
    
    Use Case:
    - Track order status changes (Pending → Completed)
    - Monitor order amount adjustments
    - Historical order analysis
    - Audit trail for order modifications
    
    Strategy: CHECK
    - Compares specified columns (check_cols) to detect changes
    - More flexible when timestamp column not available
    - Slightly less efficient than TIMESTAMP for very large tables
*/

{% snapshot orders_snapshot %}

{{
    config(
      target_database='nessie',
      target_schema='dlh_demo_snapshots',
      unique_key='order_id',
      strategy='timestamp',
      updated_at='updated_at',
      tags=['presentation', 'snapshot', 'scd']
    )
}}

SELECT
    order_id,
    customer_id,
    order_date,
    amount,
    product_category,
    order_status,
    updated_at,
    CURRENT_TIMESTAMP AS snapshot_timestamp
FROM (
    SELECT
        order_id,
        customer_id,
        order_date,
        amount,
        product_category,
        order_status,
        CAST(order_date AS TIMESTAMP) AS updated_at,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_date DESC) AS rn
    FROM {{ source('nessie', 'orders_iceberg') }}
) ranked
        WHERE rn = 1
          AND order_date >= CURRENT_DATE - INTERVAL '{{ var('snapshot_lookback_days', 30) }}' DAY

{% endsnapshot %}
