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
from {{ ref('int_ridership_daily_by_mode') }} r
left join {{ ref('dim_calendar') }} c
    on r.ride_date = c.date_day
