with stations as (
    select * from {{ ref('stg_water_quality__stations') }}
),

counties as (

    select distinct
        county_name,
        {{ map_county_name_to_county_fips('COUNTY_NAME') }} as county_fips

    from stations
    where county_fips is not null
)

select * from counties
