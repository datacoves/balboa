with raw_source as (

    select
        parse_json(replace(_airbyte_data::string,'"NaN"', 'null')) as airbyte_data_clean,
        *
    from {{ source('public', 'pcm_dps_covid19_details') }}

),

final as (

    select
        "COUNTRY_REGION" as country_region,
        "PROVINCE_STATE" as province_state,
        "DATE" as date,
        "HOSPITALIZED" as hospitalized,
        "INTENSIVE_CARE" as intensive_care,
        "TOTAL_HOSPITALIZED" as total_hospitalized,
        "HOME_ISOLATION" as home_isolation,
        "TOTAL_POSITIVE" as total_positive,
        "NEW_POSITIVE" as new_positive,
        "DISCHARGED_HEALED" as discharged_healed,
        "DECEASED" as deceased,
        "TOTAL_CASES" as total_cases,
        "TESTED" as tested,
        "HOSPITALIZED_SINCE_PREV_DAY" as hospitalized_since_prev_day,
        "INTENSIVE_CARE_SINCE_PREV_DAY" as intensive_care_since_prev_day,
        "TOTAL_HOSPITALIZED_SINCE_PREV_DAY" as total_hospitalized_since_prev_day,
        "HOME_ISOLATION_SINCE_PREV_DAY" as home_isolation_since_prev_day,
        "TOTAL_POSITIVE_SINCE_PREV_DAY" as total_positive_since_prev_day,
        "DISCHARGED_HEALED_SINCE_PREV_DAY" as discharged_healed_since_prev_day,
        "DECEASED_SINCE_PREV_DAY" as deceased_since_prev_day,
        "TOTAL_CASES_SINCE_PREV_DAY" as total_cases_since_prev_day,
        "TESTED_SINCE_PREV_DAY" as tested_since_prev_day,
        "ISO3166_1" as iso3166_1,
        "ISO3166_2" as iso3166_2,
        "NOTE_IT" as note_it,
        "NOTE_EN" as note_en,
        "LAST_UPDATE_DATE" as last_update_date,
        "LAST_REPORTED_FLAG" as last_reported_flag

    from raw_source

)

select * from final
