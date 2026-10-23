-- Heatwaves: runs of at least heatwave_min_days consecutive hot days in a cell, a hot day exceeding
-- the baseline percentile of the daily maximum temperature of its cell and calendar month
with hot_days as (
    select
        weather.h3_index,
        weather.date,
        weather.temperature_max_c,
        weather.temperature_max_c - climatology.hot_day_threshold_c as excess_c
    from {{ ref('fct_daily_cell_weather') }} as weather
    inner join {{ ref('fct_monthly_cell_climatology') }} as climatology
        on weather.h3_index = climatology.h3_index and month(weather.date) = climatology.month
    where weather.temperature_max_c > climatology.hot_day_threshold_c
),

-- Gaps and islands: consecutive dates minus their rank give the same anchor date
runs as (
    select
        *,
        date - cast(row_number() over (partition by h3_index order by date) as integer) as run_anchor
    from hot_days
)

select
    h3_index,
    min(date) as start_date,
    max(date) as end_date,
    count(*) as duration_days,
    max(temperature_max_c) as peak_temperature_max_c,
    avg(excess_c) as mean_excess_c
from runs
group by h3_index, run_anchor
having count(*) >= {{ var('heatwave_min_days') }}
