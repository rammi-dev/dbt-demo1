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
      strategy='check',
      check_cols=['order_status', 'amount'],
      invalidate_hard_deletes=True,
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
    CURRENT_TIMESTAMP AS snapshot_timestamp
FROM {{ source('nessie', 'orders_iceberg') }}
{% if var('snapshot_lookback_days', none) %}
WHERE order_date >= CURRENT_DATE - INTERVAL '{{ var('snapshot_lookback_days') }}' DAY
{% endif %}

{% endsnapshot %}
