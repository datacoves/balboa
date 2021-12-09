with raw_source as (

    select
        parse_json(replace(_airbyte_data::string,'"NaN"', 'null')) as airbyte_data_clean,
        *
    from {{ source('public', 'owid_vaccinations') }}

),

final as (

    select
        "DATE" as date,
        "COUNTRY_REGION" as country_region,
        "ISO3166_1" as iso3166_1,
        "TOTAL_VACCINATIONS" as total_vaccinations,
        "PEOPLE_VACCINATED" as people_vaccinated,
        "PEOPLE_FULLY_VACCINATED" as people_fully_vaccinated,
        "DAILY_VACCINATIONS_RAW" as daily_vaccinations_raw,
        "DAILY_VACCINATIONS" as daily_vaccinations,
        "TOTAL_VACCINATIONS_PER_HUNDRED" as total_vaccinations_per_hundred,
        "PEOPLE_VACCINATED_PER_HUNDRED" as people_vaccinated_per_hundred,
        "PEOPLE_FULLY_VACCINATED_PER_HUNDRED" as people_fully_vaccinated_per_hundred,
        "DAILY_VACCINATIONS_PER_MILLION" as daily_vaccinations_per_million,
        "VACCINES" as vaccines,
        "LAST_OBSERVATION_DATE" as last_observation_date,
        "SOURCE_NAME" as source_name,
        "SOURCE_WEBSITE" as source_website,
        "LAST_UPDATE_DATE" as last_update_date,
        "LAST_REPORTED_FLAG" as last_reported_flag

    from raw_source

)

select * from final
