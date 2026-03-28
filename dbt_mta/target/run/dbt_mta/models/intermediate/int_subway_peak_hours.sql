

  create or replace view `mta-data-491403`.`mta_warehouse`.`int_subway_peak_hours`
  OPTIONS()
  as select
    transit_date,
    station_complex_id,
    station_complex_name,
    transit_hour,
    sum(ridership) as total_ridership,
    case
        when transit_hour between 7 and 9 then 'AM Peak'
        when transit_hour between 17 and 19 then 'PM Peak'
        when transit_hour between 10 and 16 then 'Midday'
        when transit_hour between 20 and 23 then 'Evening'
        else 'Overnight'
    end as time_period
from `mta-data-491403`.`mta_warehouse`.`stg_subway_hourly`
group by 1, 2, 3, 4;

