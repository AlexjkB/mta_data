select
    station_complex_id,
    station_complex_name,
    borough,
    daytime_routes,
    latitude,
    longitude,
    is_ada_accessible
from {{ ref('stg_stations') }}
