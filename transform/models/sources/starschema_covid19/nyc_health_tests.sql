with raw_source as (

    select
        parse_json(replace(_airbyte_data::string,'"NaN"', 'null')) as airbyte_data_clean,
        *
    from {{ source('public', 'nyc_health_tests') }}

),

final as (

    select
        "MODIFIED_ZCTA" as modified_zcta,
        "COVID_CASE_COUNT" as covid_case_count,
        "TOTAL_COVID_TESTS" as total_covid_tests,
        "PERCENT_POSITIVE" as percent_positive,
        "DATE" as date,
        "FIPS" as fips,
        "COUNTRY_REGION" as country_region,
        "ISO3166_1" as iso3166_1,
        "ISO3166_2" as iso3166_2,
        "LAST_UPDATED_DATE" as last_updated_date,
        "LAST_REPORTED_DATE" as last_reported_date

    from raw_source

)

select * from final
