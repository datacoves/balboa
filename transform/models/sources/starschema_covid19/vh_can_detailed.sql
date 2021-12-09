with raw_source as (

    select
        parse_json(replace(_airbyte_data::string,'"NaN"', 'null')) as airbyte_data_clean,
        *
    from {{ source('public', 'vh_can_detailed') }}

),

final as (

    select
        "DATE" as date,
        "PROVINCE_STATE" as province_state,
        "HEALTHCARE_REGION" as healthcare_region,
        "CASES" as cases,
        "DEATHS" as deaths,
        "ISO3166_1" as iso3166_1,
        "ISO3166_2" as iso3166_2,
        "LAST_UPDATED_DATE" as last_updated_date,
        "LAST_REPORTED_FLAG" as last_reported_flag

    from raw_source

)

select * from final
