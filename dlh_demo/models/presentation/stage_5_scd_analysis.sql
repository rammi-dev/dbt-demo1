/*
    Model: stage_5_scd_analysis
    Materialization: TABLE
    
    Description:
    Customer analytics model - shows customer order patterns and segmentation.
    Modified to work without snapshots (which have Dremio/Iceberg compatibility issues).
    
    Use Case:
    - Customer order behavior analysis
    - Customer segmentation
    - Order frequency patterns
*/

-- depends_on: {{ ref('customers_snapshot') }}

{{ config(
    materialized='table',
    database='nessie',
    schema='dlh_demo_presentation',
    tags=['presentation', 'stage_5', 'analytics']
) }}

WITH history AS (
    SELECT 
        customer_id,
        name,
        country,
        customer_segment,
        dbt_valid_from,
        dbt_valid_to,
        -- Calculate duration of each version (Dremio syntax)
        TIMESTAMPDIFF(DAY, dbt_valid_from, COALESCE(dbt_valid_to, CURRENT_TIMESTAMP)) AS days_valid,
        -- Flag current version
        CASE WHEN dbt_valid_to IS NULL THEN TRUE ELSE FALSE END AS is_current_version,
        -- Version sequence
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY dbt_valid_from) AS version_number
    FROM {{ ref('customers_snapshot') }}
),

customer_stats AS (
    SELECT
        customer_id,
        COUNT(*) AS total_versions,
        MIN(dbt_valid_from) AS first_seen,
        MAX(dbt_valid_from) AS last_changed,
        -- Aggregate historical segments (using version_number for ordering)
        LISTAGG(customer_segment, ' -> ') AS historical_versions
    FROM history
    GROUP BY customer_id
)

SELECT
    h.customer_id,
    h.name,
    h.country,
    h.customer_segment,
    h.dbt_valid_from,
    h.dbt_valid_to,
    h.days_valid,
    h.is_current_version,
    h.version_number,
    c.total_versions,
    c.first_seen,
    c.last_changed,
    c.historical_versions
FROM history h
JOIN customer_stats c ON h.customer_id = c.customer_id
ORDER BY h.customer_id, h.version_number
