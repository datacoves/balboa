with raw_source as (

    select
        parse_json(replace(_airbyte_data::string,'"NaN"', 'null')) as airbyte_data_clean,
        *
    from {{ source('public', 'kff_us_reopening_timeline_increment') }}

),

final as (

    select
        "DATE" as date,
        "COUNTRY_REGION" as country_region,
        "PROVINCE_STATE" as province_state,
        "STATUS" as status

    from raw_source

)

select * from final
