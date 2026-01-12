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
      updated_at='updated_at',
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
    updated_at,
    CURRENT_TIMESTAMP AS snapshot_timestamp
FROM (
    SELECT
        customer_id,
        name,
        email,
        country,
        signup_date,
        customer_segment,
        CAST(signup_date AS TIMESTAMP) AS updated_at,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY signup_date DESC) AS rn
    FROM {{ source('nessie', 'customers_iceberg') }}
) ranked
WHERE rn = 1

{% endsnapshot %}
