{# 
  Test Configuration - Default: ERROR MODE
  - severity='error': Fails the run if test fails (current)
  - severity='warn': Shows warning only, doesn't fail the run
#}

{{ config(
    tags=['customer_orders', 'data_quality', 'row_count'],
    severity='error',
    error_if='>0'
) }}

{# 
  To switch to WARNING MODE, replace the config above with:
  
  {{ config(
      tags=['customer_orders', 'data_quality', 'row_count'],
      severity='warn',
      warn_if='>0'
  ) }}
#}

-- This test will fail if customer_orders does not have exactly 24 rows
-- Test passes when it returns 0 rows (no failures)

SELECT
    COUNT(*) as row_count
FROM {{ ref('customer_orders') }}
HAVING COUNT(*) != 24
