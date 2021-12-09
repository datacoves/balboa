with raw_source as (

    select
        parse_json(replace(_airbyte_data::string,'"NaN"', 'null')) as airbyte_data_clean,
        *
    from {{ source('public', 'jhu_covid_19_timeseries') }}

),

final as (

    select
        "COUNTRY_REGION" as country_region,
        "PROVINCE_STATE" as province_state,
        "COUNTY" as county,
        "FIPS" as fips,
        "LAT" as lat,
        "LONG" as long,
        "ISO3166_1" as iso3166_1,
        "ISO3166_2" as iso3166_2,
        "DATE" as date,
        "CASES" as cases,
        "CASE_TYPE" as case_type,
        "LAST_UPDATE_DATE" as last_update_date,
        "LAST_REPORTED_FLAG" as last_reported_flag,
        "DIFFERENCE" as difference

    from raw_source

)

select * from final
