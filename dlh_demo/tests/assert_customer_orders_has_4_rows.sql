{{ config(
    tags=['customer_orders', 'data_quality', 'row_count'],
    severity='error',
    error_if='>0'
) }}

SELECT
    COUNT(*) as row_count
FROM {{ ref('customer_orders') }}
HAVING COUNT(*) != 4
