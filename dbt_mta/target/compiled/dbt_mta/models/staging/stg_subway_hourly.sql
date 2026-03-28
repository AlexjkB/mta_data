with source as (
    select * from `mta-data-491403`.`mta_warehouse`.`raw_subway_hourly`
),

renamed as (
    select
        cast(transit_timestamp as timestamp) as transit_ts,
        date(cast(transit_timestamp as timestamp)) as transit_date,
        extract(hour from cast(transit_timestamp as timestamp)) as transit_hour,
        extract(dayofweek from cast(transit_timestamp as timestamp)) as day_of_week,
        station_complex_id,
        station_complex as station_complex_name,
        borough,
        fare_class_category,
        payment_method,
        cast(cast(ridership as float64) as int64) as ridership,
        cast(cast(transfers as float64) as int64) as transfers,
        cast(latitude as float64) as latitude,
        cast(longitude as float64) as longitude
    from source
    where transit_timestamp is not null
)

select * from renamed