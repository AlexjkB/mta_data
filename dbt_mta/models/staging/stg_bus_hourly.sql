with source as (
    select * from {{ source('raw', 'raw_bus_hourly') }}
),

renamed as (
    select
        cast(transit_timestamp as timestamp) as transit_ts,
        date(cast(transit_timestamp as timestamp)) as transit_date,
        extract(hour from cast(transit_timestamp as timestamp)) as transit_hour,
        extract(dayofweek from cast(transit_timestamp as timestamp)) as day_of_week,
        bus_route,
        fare_class_category,
        payment_method,
        cast(cast(ridership as float64) as int64) as ridership,
        cast(cast(transfers as float64) as int64) as transfers
    from source
    where transit_timestamp is not null
)

select * from renamed
