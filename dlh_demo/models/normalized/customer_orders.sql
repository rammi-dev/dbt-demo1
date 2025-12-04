{{ config(
    materialized='table',
    database='nessie',
    schema='dlh_demo_normalized',
    tags=['customers','etl']
) }}

SELECT
    c.customer_id,
    c.name,
    c.country,
    o.order_id,
    o.amount
FROM {{ source('nessie', 'customers_iceberg') }} c
LEFT JOIN {{ source('nessie', 'orders_iceberg') }} o
    ON c.customer_id = o.customer_id
