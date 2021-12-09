with raw_source as (

    select
        parse_json(replace(_airbyte_data::string,'"NaN"', 'null')) as airbyte_data_clean,
        *
    from {{ source('public', 'ecdc_global') }}

),

final as (

    select
        "COUNTRY_REGION" as country_region,
        "CONTINENTEXP" as continentexp,
        "ISO3166_1" as iso3166_1,
        "CASES" as cases,
        "DEATHS" as deaths,
        "CASES_SINCE_PREV_DAY" as cases_since_prev_day,
        "DEATHS_SINCE_PREV_DAY" as deaths_since_prev_day,
        "POPULATION" as population,
        "DATE" as date,
        "LAST_UPDATE_DATE" as last_update_date,
        "LAST_REPORTED_FLAG" as last_reported_flag

    from raw_source

)

select * from final
