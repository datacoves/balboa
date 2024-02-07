with raw_source as (

    select *
    from {{ source('LOANS', 'ZIP_COORDINATES') }}
limit 3
),

final as (

    select
        "ZIP"::string as zip,
        "LON"::float as lon,
        "LAT"::float as lat

    from raw_source

)

select * from final
