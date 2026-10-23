-- ERA5 grid points and the H3 cell containing each of them
select
    latitude,
    longitude,
    -- -180 to 180 longitude, for mapping tools
    case when longitude > 180 then longitude - 360 else longitude end as longitude_180,
    h3_index
from {{ source('era5', 'h3_lookup') }}
