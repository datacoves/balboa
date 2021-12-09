with raw_source as (

    select
        parse_json(replace(_airbyte_data::string,'"NaN"', 'null')) as airbyte_data_clean,
        *
    from {{ source('public', 'nyt_us_covid19') }}

),

final as (

    select
        "DATE" as date,
        "COUNTY" as county,
        "STATE" as state,
        "FIPS" as fips,
        "CASES" as cases,
        "DEATHS" as deaths,
        "ISO3166_1" as iso3166_1,
        "ISO3166_2" as iso3166_2,
        "CASES_SINCE_PREV_DAY" as cases_since_prev_day,
        "DEATHS_SINCE_PREV_DAY" as deaths_since_prev_day,
        "LAST_UPDATE_DATE" as last_update_date,
        "LAST_REPORTED_FLAG" as last_reported_flag

    from raw_source

)

select * from final
