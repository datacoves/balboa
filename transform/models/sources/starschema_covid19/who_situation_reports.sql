with raw_source as (

    select
        parse_json(replace(_airbyte_data::string,'"NaN"', 'null')) as airbyte_data_clean,
        *
    from {{ source('public', 'who_situation_reports') }}

),

final as (

    select
        "COUNTRY" as country,
        "TOTAL_CASES" as total_cases,
        "CASES_NEW" as cases_new,
        "DEATHS" as deaths,
        "DEATHS_NEW" as deaths_new,
        "TRANSMISSION_CLASSIFICATION" as transmission_classification,
        "DAYS_SINCE_LAST_REPORTED_CASE" as days_since_last_reported_case,
        "ISO3166_1" as iso3166_1,
        "COUNTRY_REGION" as country_region,
        "DATE" as date,
        "SITUATION_REPORT_NAME" as situation_report_name,
        "SITUATION_REPORT_URL" as situation_report_url,
        "LAST_UPDATE_DATE" as last_update_date,
        "LAST_REPORTED_FLAG" as last_reported_flag

    from raw_source

)

select * from final
