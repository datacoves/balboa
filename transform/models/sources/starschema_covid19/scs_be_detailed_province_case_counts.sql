with raw_source as (

    select
        parse_json(replace(_airbyte_data::string,'"NaN"', 'null')) as airbyte_data_clean,
        *
    from {{ source('public', 'scs_be_detailed_province_case_counts') }}

),

final as (

    select
        "PROVINCE" as province,
        "REGION" as region,
        "SEX" as sex,
        "AGEGROUP" as agegroup,
        "DATE" as date,
        "ISO3166_1" as iso3166_1,
        "ISO3166_2" as iso3166_2,
        "ISO3166_3" as iso3166_3,
        "NEW_CASES" as new_cases,
        "TOTAL_CASES" as total_cases,
        "LAST_UPDATED_DATE" as last_updated_date

    from raw_source

)

select * from final
