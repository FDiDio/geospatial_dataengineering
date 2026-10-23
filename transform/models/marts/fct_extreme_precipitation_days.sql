-- Extreme precipitation days: wet days above the given percentile of the wet days of the same cell
with thresholds as (
    select
        h3_index,
        quantile_cont(precipitation_mm, {{ var('extreme_precipitation_percentile') }}) as extreme_threshold_mm,
        count(*) as wet_days
    from {{ ref('fct_daily_cell_weather') }}
    where is_wet_day and {{ baseline_years('date') }}
    group by h3_index
)

select
    weather.date,
    weather.h3_index,
    weather.precipitation_mm,
    weather.precipitation_max_hourly_mm,
    thresholds.extreme_threshold_mm,
    weather.precipitation_mm / thresholds.extreme_threshold_mm as ratio_to_threshold
from {{ ref('fct_daily_cell_weather') }} as weather
inner join thresholds using (h3_index)
where weather.is_wet_day and weather.precipitation_mm > thresholds.extreme_threshold_mm
