with raw_source as (

    select
        parse_json(replace(_airbyte_data::string,'"NaN"', 'null')) as airbyte_data_clean,
        *
    from {{ source('public', 'ecdc_global_weekly') }}

),

final as (

    select
        "COUNTRY_REGION" as country_region,
        "CONTINENTEXP" as continentexp,
        "ISO3166_1" as iso3166_1,
        "CASES_WEEKLY" as cases_weekly,
        "DEATHS_WEEKLY" as deaths_weekly,
        "CASES_SINCE_PREV_WEEK" as cases_since_prev_week,
        "DEATHS_SINCE_PREV_WEEK" as deaths_since_prev_week,
        "POPULATION" as population,
        "DATE" as date,
        "LAST_UPDATE_DATE" as last_update_date,
        "LAST_REPORTED_FLAG" as last_reported_flag

    from raw_source

)

select * from final
