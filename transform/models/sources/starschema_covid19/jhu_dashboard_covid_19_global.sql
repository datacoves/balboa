with raw_source as (

    select
        parse_json(replace(_airbyte_data::string,'"NaN"', 'null')) as airbyte_data_clean,
        *
    from {{ source('public', 'jhu_dashboard_covid_19_global') }}

),

final as (

    select
        "COUNTRY_REGION" as country_region,
        "PROVINCE_STATE" as province_state,
        "COUNTY" as county,
        "FIPS" as fips,
        "DATE" as date,
        "ACTIVE" as active,
        "PEOPLE_TESTED" as people_tested,
        "CONFIRMED" as confirmed,
        "PEOPLE_HOSPITALIZED" as people_hospitalized,
        "DEATHS" as deaths,
        "RECOVERED" as recovered,
        "INCIDENT_RATE" as incident_rate,
        "TESTING_RATE" as testing_rate,
        "HOSPITALIZATION_RATE" as hospitalization_rate,
        "MORTALITY_RATE" as mortality_rate,
        "LONG" as long,
        "LAT" as lat,
        "ISO3166_1" as iso3166_1,
        "ISO3166_2" as iso3166_2,
        "LAST_UPDATE_DATE" as last_update_date,
        "LAST_REPORTED_FLAG" as last_reported_flag

    from raw_source

)

select * from final
