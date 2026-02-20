with stations as (
    select * from {{ ref('stg_water_quality__stations') }}
),

lab_results as (
    select * from {{ ref('stg_water_quality__lab_results') }}
),

stations_per_county as (
    select
        station_id,
        county_name

    from stations
    group by station_id, county_name
),

 dissolved_chloride_stations_per_county_counted as (
    select
        s.county_name,
        count(distinct l.station_id) as station_count

    from stations_per_county as s
    inner join lab_results as l
        on s.station_id = l.station_id

    where
        year(l.sample_date) = 2024
        and l.parameter = 'Dissolved Chloride'
    group by s.county_name
)

select * from dissolved_chloride_stations_per_county_counted
order by station_count desc
