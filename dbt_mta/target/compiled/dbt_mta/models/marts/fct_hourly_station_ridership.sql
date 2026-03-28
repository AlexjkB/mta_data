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
from `mta-data-491403`.`mta_warehouse`.`int_subway_peak_hours` p
left join `mta-data-491403`.`mta_warehouse`.`dim_stations` s
    on p.station_complex_id = s.station_complex_id