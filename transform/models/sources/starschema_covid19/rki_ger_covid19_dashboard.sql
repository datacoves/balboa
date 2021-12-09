with raw_source as (

    select
        parse_json(replace(_airbyte_data::string,'"NaN"', 'null')) as airbyte_data_clean,
        *
    from {{ source('public', 'rki_ger_covid19_dashboard') }}

),

final as (

    select
        "DISTRICT_ID" as district_id,
        "COUNTY" as county,
        "STATE_ID" as state_id,
        "STATE" as state,
        "DISTRICT_TYPE" as district_type,
        "POPULATION" as population,
        "DEATH_RATE" as death_rate,
        "CASES" as cases,
        "DEATHS" as deaths,
        "CASES_PER_100K" as cases_per_100k,
        "CASES_PER_POPULATION" as cases_per_population,
        "CASES7_PER_100K" as cases7_per_100k,
        "LAST_UPDATE" as last_update,
        "ISO3166_1" as iso3166_1,
        "ISO3166_2" as iso3166_2,
        "LAST_UPDATE_DATE" as last_update_date,
        "LAST_REPORTED_FLAG" as last_reported_flag

    from raw_source

)

select * from final
