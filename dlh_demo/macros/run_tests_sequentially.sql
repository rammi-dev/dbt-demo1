{% macro run_tests_sequentially(model_name) %}
  {#
    Macro to run tests in a specific order and stop on first failure
    
    Usage:
    dbt run-operation run_tests_sequentially --args '{model_name: customer_orders}'
  #}
  
  {% set tests_in_order = [
    'not_null_customer_orders_customer_id',
    'dbt_expectations_expect_column_values_to_not_be_null_customer_orders_name',
    'assert_customer_orders_has_4_rows'
  ] %}
  
  {{ log("Running tests sequentially for model: " ~ model_name, info=True) }}
  
  {% for test_name in tests_in_order %}
    {{ log("Running test: " ~ test_name, info=True) }}
    
    {% set test_query %}
      -- You would need to implement the actual test query here
      SELECT 1 as placeholder
    {% endset %}
    
    {# This is a simplified example - actual implementation would be more complex #}
    {{ log("Test " ~ test_name ~ " completed", info=True) }}
  {% endfor %}
  
{% endmacro %}
