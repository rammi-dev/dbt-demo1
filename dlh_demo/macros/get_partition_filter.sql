{#
    Macro: get_partition_filter
    
    Description:
    Returns a SQL WHERE clause for partition filtering based on environment variables or model config.
    Supports both single date and date range filtering for Iceberg tables in Dremio.
    
    Parameters:
    - partition_column: Name of the date/timestamp column used for partitioning (default: 'order_date')
    - start_date_var: Environment variable for start date (default: 'DBT_START_DATE')
    - end_date_var: Environment variable for end date (default: 'DBT_END_DATE')
    - default_days_back: Number of days to look back if env vars not set (default: 7)
    - use_range: Whether to use date range (true) or single start date (false) (default: true)
    
    Usage Examples:
    
    1. Date range with env vars:
       export DBT_START_DATE='2024-01-01'
       export DBT_END_DATE='2024-01-31'
       WHERE {{ get_partition_filter('order_date') }}
       -- Results in: order_date >= DATE '2024-01-01' AND order_date < DATE '2024-02-01'
    
    2. Single date threshold:
       WHERE {{ get_partition_filter('order_date', use_range=false, default_days_back=30) }}
       -- Results in: order_date >= CURRENT_DATE - INTERVAL '30' DAY
    
    3. Custom env var names:
       WHERE {{ get_partition_filter('created_at', 'START_TS', 'END_TS') }}
    
    Returns:
    SQL WHERE condition as a string
#}

{% macro get_partition_filter(
    partition_column='order_date',
    start_date_var='DBT_START_DATE',
    end_date_var='DBT_END_DATE',
    default_days_back=7,
    use_range=true
) %}
    
    {# Get environment variables #}
    {% set start_date = env_var(start_date_var, '') %}
    {% set end_date = env_var(end_date_var, '') %}
    
    {# Log what we're using for debugging #}
    {{ log("Partition filter - Column: " ~ partition_column ~ ", Start: " ~ start_date ~ ", End: " ~ end_date, info=false) }}
    
    {% if use_range %}
        {# Date range filtering #}
        {% if start_date and end_date %}
            {# Both dates provided - use exact range #}
            {{ partition_column }} >= DATE '{{ start_date }}' AND {{ partition_column }} < DATE '{{ end_date }}'
        {% elif start_date %}
            {# Only start date provided - filter from start date onwards #}
            {{ partition_column }} >= DATE '{{ start_date }}'
        {% else %}
            {# No dates provided - use default lookback window #}
            {{ partition_column }} >= CURRENT_DATE - INTERVAL '{{ default_days_back }}' DAY 
            AND {{ partition_column }} < CURRENT_DATE + INTERVAL '1' DAY
        {% endif %}
    {% else %}
        {# Simple threshold filtering (>= start date only) #}
        {% if start_date %}
            {{ partition_column }} >= DATE '{{ start_date }}'
        {% else %}
            {{ partition_column }} >= CURRENT_DATE - INTERVAL '{{ default_days_back }}' DAY
        {% endif %}
    {% endif %}

{% endmacro %}
