with raw_source as (

    select
        parse_json(replace(_airbyte_data::string,'"NaN"', 'null')) as airbyte_data_clean,
        *
    from {{ source('public', 'kff_us_icu_beds') }}

),

final as (

    select
        "COUNTRY_REGION" as country_region,
        "ISO3166_1" as iso3166_1,
        "ISO3166_2" as iso3166_2,
        "NOTE" as note,
        "STATE" as state,
        "HOSPITALS" as hospitals,
        "ICU_BEDS" as icu_beds,
        "COUNTY" as county,
        "FIPS" as fips

    from raw_source

)

select * from final
