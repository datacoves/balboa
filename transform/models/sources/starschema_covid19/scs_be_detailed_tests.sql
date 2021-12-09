with raw_source as (

    select
        parse_json(replace(_airbyte_data::string,'"NaN"', 'null')) as airbyte_data_clean,
        *
    from {{ source('public', 'scs_be_detailed_tests') }}

),

final as (

    select
        "DATE" as date,
        "TESTS" as tests,
        "LAST_UPDATED_DATE" as last_updated_date

    from raw_source

)

select * from final
