{#-
    Filter of the incremental models on a date or timestamp column:
    - with the start_date (and optionally end_date) vars, exactly those days, e.g. a Dagster partition;
    - on an incremental run, the days after the last one built, minus lookback_days, to pick up late data;
    - on a full refresh, every day.
-#}
{% macro incremental_window(column) -%}
    {%- if var('start_date') -%}
        {{ column }} >= '{{ var("start_date") }}'::timestamp
        and {{ column }} < '{{ var("end_date") or var("start_date") }}'::timestamp + interval 1 day
    {%- elif is_incremental() -%}
        {{ column }} >= (
            select coalesce(max(date), '1900-01-01'::date) - interval {{ var('lookback_days') }} day from {{ this }}
        )
    {%- else -%}
        true
    {%- endif -%}
{%- endmacro %}
