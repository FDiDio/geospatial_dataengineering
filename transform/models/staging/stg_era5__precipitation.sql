-- Hourly precipitation in mm. ERA5 labels an hourly accumulation with the end of the hour,
-- so the 24 values of a calendar day cover 23:00 of the previous day to 23:00 UTC.
select
    time as observed_at,
    cast(time as date) as date,
    latitude,
    longitude,
    h3_index,
    tp * 1000 as precipitation_mm
from {{ source('era5', 'total_precipitation') }}
