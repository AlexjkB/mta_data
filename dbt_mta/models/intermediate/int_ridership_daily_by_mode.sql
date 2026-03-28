select ride_date, 'Subway' as transport_mode, subway_ridership as ridership, subway_pct_pre_pandemic as pct_pre_pandemic
from {{ ref('stg_daily_ridership') }}
union all
select ride_date, 'Bus', bus_ridership, bus_pct_pre_pandemic
from {{ ref('stg_daily_ridership') }}
union all
select ride_date, 'LIRR', lirr_ridership, lirr_pct_pre_pandemic
from {{ ref('stg_daily_ridership') }}
union all
select ride_date, 'Metro-North', metro_north_ridership, metro_north_pct_pre_pandemic
from {{ ref('stg_daily_ridership') }}
union all
select ride_date, 'SIR', sir_ridership, null
from {{ ref('stg_daily_ridership') }}
union all
select ride_date, 'Access-A-Ride', access_a_ride_ridership, null
from {{ ref('stg_daily_ridership') }}
union all
select ride_date, 'Bridges & Tunnels', bridges_tunnels_traffic, null
from {{ ref('stg_daily_ridership') }}
