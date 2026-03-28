select
    transit_date,
    station_complex_id,
    station_complex_name,
    sum(ridership) as daily_ridership,
    sum(transfers) as daily_transfers
from {{ ref('stg_subway_hourly') }}
group by 1, 2, 3
