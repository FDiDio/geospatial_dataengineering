-- Daily precipitation of each H3 cell: the mean of the daily totals of its grid points (areal mean)
with grid_point_totals as (
    select
        date,
        h3_index,
        latitude,
        longitude,
        sum(precipitation_mm) as precipitation_mm,
        max(precipitation_mm) as precipitation_max_hourly_mm,
        count(precipitation_mm) as hourly_values
    from {{ ref('stg_era5__precipitation') }}
    where {{ incremental_window('observed_at') }}
    group by date, h3_index, latitude, longitude
)

select
    date,
    h3_index,
    avg(precipitation_mm) as precipitation_mm,
    max(precipitation_max_hourly_mm) as precipitation_max_hourly_mm,
    sum(hourly_values) as hourly_values
from grid_point_totals
group by date, h3_index
