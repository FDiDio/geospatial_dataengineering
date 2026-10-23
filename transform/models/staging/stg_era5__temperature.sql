-- Hourly 2m temperature in °C
select
    time as observed_at,
    cast(time as date) as date,
    latitude,
    longitude,
    h3_index,
    t2m - 273.15 as temperature_c
from {{ source('era5', '2m_temperature') }}
