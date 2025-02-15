with raw_source as (

    select *
    from {{ source('LOANS', 'ZIP_COORDINATES') }}

),

final as (

    select
        "ZIP"::number as zip,
        "LAT"::float as lat,
        "LON"::float as lon

    from raw_source

)

select * from final
