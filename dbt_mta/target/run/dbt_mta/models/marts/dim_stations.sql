
  
    

    create or replace table `mta-data-491403`.`mta_warehouse`.`dim_stations`
      
    
    

    
    OPTIONS()
    as (
      select
    station_complex_id,
    station_complex_name,
    borough,
    daytime_routes,
    latitude,
    longitude,
    is_ada_accessible
from `mta-data-491403`.`mta_warehouse`.`stg_stations`
    );
  