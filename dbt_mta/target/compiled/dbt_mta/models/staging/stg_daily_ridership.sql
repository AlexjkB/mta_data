with source as (
    select * from `mta-data-491403`.`mta_warehouse`.`raw_daily_ridership`
),

renamed as (
    select
        cast(cast(date as timestamp) as date) as ride_date,
        cast(subways_total_estimated_ridership as int64) as subway_ridership,
        cast(subways_of_comparable_pre_pandemic_day as float64) as subway_pct_pre_pandemic,
        cast(buses_total_estimated_ridersip as int64) as bus_ridership,
        cast(buses_of_comparable_pre_pandemic_day as float64) as bus_pct_pre_pandemic,
        cast(lirr_total_estimated_ridership as int64) as lirr_ridership,
        cast(lirr_of_comparable_pre_pandemic_day as float64) as lirr_pct_pre_pandemic,
        cast(metro_north_total_estimated_ridership as int64) as metro_north_ridership,
        cast(metro_north_of_comparable_pre_pandemic_day as float64) as metro_north_pct_pre_pandemic,
        cast(access_a_ride_total_scheduled_trips as int64) as access_a_ride_ridership,
        cast(bridges_and_tunnels_total_traffic as int64) as bridges_tunnels_traffic,
        cast(staten_island_railway_total_estimated_ridership as int64) as sir_ridership
    from source
    where date is not null
)

select * from renamed