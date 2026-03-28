
  
    

    create or replace table `mta-data-491403`.`mta_warehouse`.`fct_daily_ridership`
      
    
    

    
    OPTIONS()
    as (
      select
    r.ride_date,
    r.transport_mode,
    r.ridership,
    r.pct_pre_pandemic,
    c.day_name,
    c.is_weekend,
    c.month_name,
    c.quarter,
    c.year
from `mta-data-491403`.`mta_warehouse`.`int_ridership_daily_by_mode` r
left join `mta-data-491403`.`mta_warehouse`.`dim_calendar` c
    on r.ride_date = c.date_day
    );
  