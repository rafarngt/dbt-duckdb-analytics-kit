{% macro get_custom_schema(node_custom_schema) -%}
    {%- set default_schema = target.schema -%}
    {%- if node_custom_schema is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ default_schema }}_{{ node_custom_schema | trim }}
    {%- endif -%}
{%- endmacro %}