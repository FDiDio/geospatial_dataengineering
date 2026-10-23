{{ config(materialized='incremental', incremental_strategy='delete+insert', unique_key='date') }}

-- Daily weather rolled up to coarser H3 regions (resolution 3, ~12,000 km²): light enough for global maps
select
    weather.date,
    cell.region_h3_index,
    -- area-weighted means (cells of the same resolution still differ in area), over the cells with a value
    sum(weather.temperature_mean_c * cell.area_km2)
        / sum(cell.area_km2) filter (where weather.temperature_mean_c is not null) as temperature_mean_c,
    min(weather.temperature_min_c) as temperature_min_c,
    max(weather.temperature_max_c) as temperature_max_c,
    sum(weather.precipitation_mm * cell.area_km2)
        / sum(cell.area_km2) filter (where weather.precipitation_mm is not null) as precipitation_mm,
    max(weather.precipitation_mm) as precipitation_max_cell_mm,
    count(*) as cells
from {{ ref('fct_daily_cell_weather') }} as weather
inner join {{ ref('dim_h3_cell') }} as cell using (h3_index)
where {{ incremental_window('weather.date') }}
group by weather.date, cell.region_h3_index
