with stations as (

    select
        to_varchar(station_id) as station_id,

        full_station_name,
        station_number,
        station_type,
        latitude,
        longitude,
        county_name,
        sample_count,

        to_timestamp(sample_date_min, 'MM/DD/YYYY HH24:MI') as sample_timestamp_min,
        to_timestamp(sample_date_max, 'MM/DD/YYYY HH24:MI') as sample_timestamp_max

    from {{ source('WATER_QUALITY', 'stations') }}
)

select * from stations
