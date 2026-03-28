with source as (
    select * from `mta-data-491403`.`mta_warehouse`.`raw_bridges_tunnels`
),

renamed as (
    select
        plaza,
        date(cast(collection_date as timestamp)) as collection_date,
        cast(day_of_week as int64) as day_of_week,
        cast(holiday as int64) as is_holiday,
        cast(total as int64) as total_traffic,
        cast(class_1 as int64) as passenger_vehicles,
        cast(class_2 as int64) as class_2_vehicles,
        cast(class_3 as int64) as class_3_vehicles
    from source
    where collection_date is not null
)

select * from renamed