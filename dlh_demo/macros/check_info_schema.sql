{% macro check_info_schema() %}

{% set tables_query %}
SELECT * FROM INFORMATION_SCHEMA."TABLES" LIMIT 0
{% endset %}

{% set columns_query %}
SELECT * FROM INFORMATION_SCHEMA."COLUMNS" LIMIT 0
{% endset %}

{{ log("Checking INFORMATION_SCHEMA.TABLES columns:", info=True) }}
{% set tables_result = run_query(tables_query) %}
{% if execute %}
  {% set table_cols = tables_result.column_names %}
  {{ log("Actual columns: " ~ table_cols|join(', '), info=True) }}
{% endif %}

{{ log("", info=True) }}
{{ log("Checking INFORMATION_SCHEMA.COLUMNS columns:", info=True) }}
{% set columns_result = run_query(columns_query) %}
{% if execute %}
  {% set col_cols = columns_result.column_names %}
  {{ log("Actual columns: " ~ col_cols|join(', '), info=True) }}
{% endif %}

{% endmacro %}
