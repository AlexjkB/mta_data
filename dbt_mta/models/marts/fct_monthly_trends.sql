select
    date_trunc(ride_date, month) as month_start,
    transport_mode,
    sum(ridership) as monthly_ridership,
    avg(pct_pre_pandemic) as avg_pct_pre_pandemic,
    count(*) as days_in_month
from {{ ref('int_ridership_daily_by_mode') }}
group by 1, 2
