{% macro drop_temp_table(relation_name) %}
  {#
    Macro to drop leftover temp tables from failed incremental runs.
    
    dbt-dremio adapter doesn't always clean up __dbt_tmp tables on failure.
    Call this before incremental runs if you encounter "table already exists" errors.
    
    Usage:
    dbt run-operation drop_temp_table --args '{"relation_name": "stage_3_incremental"}'
    
    Or in a pre-hook:
    {{ config(
        pre_hook="{{ drop_temp_table(this.identifier) }}"
    ) }}
  #}
  
  {% set temp_table = relation_name ~ '__dbt_tmp' %}
  {% set schema = target.schema if target.schema else 'dlh_demo_presentation' %}
  {% set database = 'nessie' %}
  
  {{ log("Attempting to drop temp table: " ~ database ~ "." ~ schema ~ ".\"" ~ temp_table ~ "\"", info=True) }}
  
  {% set drop_query %}
    DROP TABLE IF EXISTS {{ database }}.{{ schema }}."{{ temp_table }}"
  {% endset %}
  
  {% do run_query(drop_query) %}
  
  {{ log("Temp table cleanup complete", info=True) }}
  
{% endmacro %}


{% macro drop_all_temp_tables() %}
  {#
    Macro to drop all known temp tables from failed incremental runs.
    
    Usage:
    dbt run-operation drop_all_temp_tables
  #}
  
  {% set temp_tables = [
    'stage_3_incremental__dbt_tmp'
  ] %}
  
  {% set database = 'nessie' %}
  {% set schema = 'dlh_demo_presentation' %}
  
  {% for temp_table in temp_tables %}
    {{ log("Dropping: " ~ database ~ "." ~ schema ~ ".\"" ~ temp_table ~ "\"", info=True) }}
    
    {% set drop_query %}
      DROP TABLE IF EXISTS {{ database }}.{{ schema }}."{{ temp_table }}"
    {% endset %}
    
    {% do run_query(drop_query) %}
  {% endfor %}
  
  {{ log("All temp tables cleaned up", info=True) }}
  
{% endmacro %}
