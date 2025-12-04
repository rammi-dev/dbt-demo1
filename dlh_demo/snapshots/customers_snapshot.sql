/*
    Snapshot: customers_snapshot
    Strategy: TIMESTAMP
    
    Description:
    This snapshot tracks slowly changing dimensions (SCD Type 2) for customer data.
    It captures historical changes to customer records over time using timestamp strategy.
    
    Each time a customer's data changes (name, email, country, or segment), a new record
    is created with updated dbt_valid_from and dbt_valid_to timestamps.
    
    Use Case:
    - Track customer attribute changes over time
    - Historical reporting and analysis
    - Audit trail for customer data
    - Point-in-time customer segmentation
    
    Strategy: TIMESTAMP
    - Uses updated_at column to detect changes
    - More efficient than CHECK strategy for large tables
    - Requires source table to have an updated_at timestamp
*/

{% snapshot customers_snapshot %}

{{
    config(
      target_database='nessie',
      target_schema='dlh_demo_snapshots',
      unique_key='customer_id',
      strategy='timestamp',
      updated_at='signup_date',
      invalidate_hard_deletes=True,
      tags=['presentation', 'snapshot', 'scd']
    )
}}

SELECT
    customer_id,
    name,
    email,
    country,
    signup_date,
    customer_segment,
    CURRENT_TIMESTAMP AS snapshot_timestamp
FROM {{ source('nessie', 'customers_iceberg') }}

{% endsnapshot %}
