with raw_source as (

    select
        parse_json(replace(_airbyte_data::string,'"NaN"', 'null')) as airbyte_data_clean,
        *
    from {{ source('public', 'who_timeseries') }}

),

final as (

    select
        "COUNTRY_REGION" as country_region,
        "CASES_TOTAL" as cases_total,
        "CASES_TOTAL_PER_100000" as cases_total_per_100000,
        "CASES" as cases,
        "DEATHS_TOTAL" as deaths_total,
        "DEATHS_TOTAL_PER_100000" as deaths_total_per_100000,
        "DEATHS" as deaths,
        "TRANSMISSION_CLASSIFICATION" as transmission_classification,
        "DATE" as date,
        "ISO3166_1" as iso3166_1

    from raw_source

)

select * from final
