{#- Use the custom schema as is (staging, intermediate, marts) instead of prefixing it with the target schema -#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ custom_schema_name if custom_schema_name is not none else target.schema }}
{%- endmacro %}
