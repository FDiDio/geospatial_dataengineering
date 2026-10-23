-- Every cell with weather data on a day is counted in exactly one region of that day
with cells as (
    select date, count(*) as cells from {{ ref('fct_daily_cell_weather') }} group by date
),

regions as (
    select date, sum(cells) as cells from {{ ref('agg_daily_region_weather') }} group by date
)

select cells.date, cells.cells, regions.cells as cells_in_regions
from cells
left join regions using (date)
where regions.cells is distinct from cells.cells
