{% macro date_diff(first_date, second_date, datepart) %}
  {{ dbt.datediff(first_date, second_date, datepart) }}
{% endmacro %}

{% macro current_timestamp() %}
  current_timestamp
{% endmacro %}

{% macro current_date() %}
  current_date
{% endmacro %}

{% macro date_trunc(datepart, date) %}
  date_trunc('{{ datepart }}', {{ date }})
{% endmacro %}