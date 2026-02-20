with lab_results as (

    select
        to_varchar(station_id) as station_id,
        status,
        sample_code,

        to_timestamp(sample_date, 'MM/DD/YYYY HH24:MI') as sample_timestamp,

        date_from_parts(
            substr(sample_date, 7, 4)::INT,
            left(sample_date, 2)::INT,
            substr(sample_date, 4, 2)::INT
        ) as sample_date,

        sample_depth,
        sample_depth_units,
        parameter,
        result,
        reporting_limit,
        units,
        method_name

    from {{ source('WATER_QUALITY', 'lab_results') }}
)

select * from lab_results
