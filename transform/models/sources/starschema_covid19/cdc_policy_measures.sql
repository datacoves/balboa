with raw_source as (

    select
        parse_json(replace(_airbyte_data::string,'"NaN"', 'null')) as airbyte_data_clean,
        *
    from {{ source('public', 'cdc_policy_measures') }}

),

final as (

    select
        "STATE_ID" as state_id,
        "COUNTY" as county,
        "FIPS_CODE" as fips_code,
        "POLICY_LEVEL" as policy_level,
        "DATE" as date,
        "POLICY_TYPE" as policy_type,
        "START_STOP" as start_stop,
        "COMMENTS" as comments,
        "SOURCE" as source,
        "TOTAL_PHASE" as total_phase,
        "ISO3166_1" as iso3166_1,
        "ISO3166_2" as iso3166_2,
        "LAST_UPDATE_DATE" as last_update_date,
        "LAST_REPORTED_FLAG" as last_reported_flag

    from raw_source

)

select * from final
