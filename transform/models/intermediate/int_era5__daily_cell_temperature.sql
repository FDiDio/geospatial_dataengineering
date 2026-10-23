-- Daily temperature statistics of each H3 cell, over all the hourly values of its grid points
select
    date,
    h3_index,
    avg(temperature_c) as temperature_mean_c,
    min(temperature_c) as temperature_min_c,
    max(temperature_c) as temperature_max_c,
    count(temperature_c) as hourly_values
from {{ ref('stg_era5__temperature') }}
where {{ incremental_window('observed_at') }}
group by date, h3_index
