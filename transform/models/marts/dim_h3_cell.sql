-- H3 cells covering the ERA5 grid, with their geometry and their region of the rollup
select
    h3_index,
    h3_cell_to_lat(h3_index) as center_latitude,
    h3_cell_to_lng(h3_index) as center_longitude,
    h3_cell_area(h3_index, 'km^2') as area_km2,
    h3_cell_to_parent(h3_index, {{ var('region_h3_resolution') }}) as region_h3_index,
    count(*) as grid_points
from {{ ref('stg_era5__grid_points') }}
group by h3_index
