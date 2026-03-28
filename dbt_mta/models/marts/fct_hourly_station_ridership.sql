select
    p.transit_date,
    p.station_complex_id,
    p.station_complex_name,
    p.transit_hour,
    p.time_period,
    p.total_ridership,
    s.borough,
    s.latitude,
    s.longitude
from {{ ref('int_subway_peak_hours') }} p
left join {{ ref('dim_stations') }} s
    on p.station_complex_id = s.station_complex_id
