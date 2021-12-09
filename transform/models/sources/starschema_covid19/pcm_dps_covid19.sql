with raw_source as (

    select
        parse_json(replace(_airbyte_data::string,'"NaN"', 'null')) as airbyte_data_clean,
        *
    from {{ source('public', 'pcm_dps_covid19') }}

),

final as (

    select
        "COUNTRY_REGION" as country_region,
        "PROVINCE_STATE" as province_state,
        "DATE" as date,
        "CASE_TYPE" as case_type,
        "CASES" as cases,
        "LONG" as long,
        "LAT" as lat,
        "DIFFERENCE" as difference,
        "ISO3166_1" as iso3166_1,
        "ISO3166_2" as iso3166_2,
        "LAST_UPDATED_DATE" as last_updated_date,
        "LAST_REPORTED_FLAG" as last_reported_flag

    from raw_source

)

select * from final
