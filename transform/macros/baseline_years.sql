{#- Restrict a date column to the years of the climatological baseline -#}
{% macro baseline_years(column) -%}
    year({{ column }}) between {{ var('baseline_start_year') or 0 }} and {{ var('baseline_end_year') or 9999 }}
{%- endmacro %}
