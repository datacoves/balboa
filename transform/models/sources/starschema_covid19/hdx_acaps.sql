with raw_source as (

    select
        parse_json(replace(_airbyte_data::string,'"NaN"', 'null')) as airbyte_data_clean,
        *
    from {{ source('public', 'hdx_acaps') }}

),

final as (

    select
        "COUNTRY_STATE" as country_state,
        "ADMIN_2" as admin_2,
        "REGION" as region,
        "CATEGORY" as category,
        "MEASURE" as measure,
        "TARGETED_POP_GROUP" as targeted_pop_group,
        "COMMENTS" as comments,
        "NON_COMPLIANCE" as non_compliance,
        "DATE_IMPLEMENTED" as date_implemented,
        "SOURCE" as source,
        "SOURCE_TYPE" as source_type,
        "LINK" as link,
        "ENTRY_DATE" as entry_date,
        "ISO3166_1" as iso3166_1,
        "LAST_UPDATED_DATE" as last_updated_date,
        "LOG_TYPE" as log_type

    from raw_source

)

select * from final
