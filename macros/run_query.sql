{% macro execute_query(query) %}
    {% set results = run_query(query) %}
    {% do results.print_table() %}
    {{ return(results) }}
{% endmacro %} 