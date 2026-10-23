{{ config(materialized='incremental', incremental_strategy='delete+insert', unique_key='date') }}

-- Daily weather of each H3 cell: one row per cell and day
with temperature as (
    select * from {{ ref('int_era5__daily_cell_temperature') }}
    where {{ incremental_window('date') }}
),

precipitation as (
    select * from {{ ref('int_era5__daily_cell_precipitation') }}
    where {{ incremental_window('date') }}
)

select
    date,
    h3_index,
    temperature.temperature_mean_c,
    temperature.temperature_min_c,
    temperature.temperature_max_c,
    temperature.temperature_max_c - temperature.temperature_min_c as temperature_range_c,
    precipitation.precipitation_mm,
    precipitation.precipitation_max_hourly_mm,
    precipitation.precipitation_mm >= {{ var('wet_day_mm') }} as is_wet_day
from temperature
full outer join precipitation using (date, h3_index)
