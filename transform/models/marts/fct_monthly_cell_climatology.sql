-- Climatological baseline of each H3 cell and calendar month, over the baseline years
select
    h3_index,
    month(date) as month,
    avg(temperature_mean_c) as temperature_mean_c,
    stddev_samp(temperature_mean_c) as temperature_mean_stddev_c,
    avg(temperature_max_c) as temperature_max_mean_c,
    quantile_cont(temperature_max_c, {{ var('heatwave_percentile') }}) as hot_day_threshold_c,
    avg(precipitation_mm) as precipitation_mean_mm,
    avg(is_wet_day::int) as wet_day_frequency,
    count(distinct year(date)) as years,
    count(*) as days
from {{ ref('fct_daily_cell_weather') }}
where {{ baseline_years('date') }}
group by h3_index, month(date)
