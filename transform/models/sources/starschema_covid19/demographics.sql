with raw_source as (

    select
        parse_json(replace(_airbyte_data::string,'"NaN"', 'null')) as airbyte_data_clean,
        *
    from {{ source('public', 'demographics') }}

),

final as (

    select
        "ISO3166_1" as iso3166_1,
        "ISO3166_2" as iso3166_2,
        "FIPS" as fips,
        "LATITUDE" as latitude,
        "LONGITUDE" as longitude,
        "STATE" as state,
        "COUNTY" as county,
        "TOTAL_POPULATION" as total_population,
        "TOTAL_MALE_POPULATION" as total_male_population,
        "TOTAL_FEMALE_POPULATION" as total_female_population

    from raw_source

)

select * from final
