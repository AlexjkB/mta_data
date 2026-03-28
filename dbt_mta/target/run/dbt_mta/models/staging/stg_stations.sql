

  create or replace view `mta-data-491403`.`mta_warehouse`.`stg_stations`
  OPTIONS()
  as with source as (
    select * from `mta-data-491403`.`mta_warehouse`.`raw_stations`
),

renamed as (
    select
        complex_id as station_complex_id,
        display_name as station_complex_name,
        borough,
        daytime_routes,
        cast(latitude as float64) as latitude,
        cast(longitude as float64) as longitude,
        case when ada = '1' then true else false end as is_ada_accessible
    from source
)

select * from renamed;

