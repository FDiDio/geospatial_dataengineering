{{ config(materialized='view') }}

-- Daily mean temperature of each cell compared with its baseline for the calendar month
select
    weather.date,
    weather.h3_index,
    weather.temperature_mean_c,
    climatology.temperature_mean_c as baseline_temperature_mean_c,
    weather.temperature_mean_c - climatology.temperature_mean_c as temperature_anomaly_c,
    -- standardized anomaly: how many standard deviations from the baseline
    (weather.temperature_mean_c - climatology.temperature_mean_c)
        / nullif(climatology.temperature_mean_stddev_c, 0) as temperature_anomaly_z
from {{ ref('fct_daily_cell_weather') }} as weather
inner join {{ ref('fct_monthly_cell_climatology') }} as climatology
    on weather.h3_index = climatology.h3_index and month(weather.date) = climatology.month
