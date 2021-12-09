with raw_source as (

    select
        parse_json(replace(_airbyte_data::string,'"NaN"', 'null')) as airbyte_data_clean,
        *
    from {{ source('public', 'kff_us_state_mitigations') }}

),

final as (

    select
        "COUNTRY_REGION" as country_region,
        "PROVINCE_STATE" as province_state,
        "STATUS_OF_REOPENING" as status_of_reopening,
        "STAY_AT_HOME_ORDER" as stay_at_home_order,
        "MANDATORY_QUARANTINE_FOR_TRAVELERS" as mandatory_quarantine_for_travelers,
        "NON_ESSENTIAL_BUSINESS_CLOSURES" as non_essential_business_closures,
        "LARGE_GATHERINGS_BAN" as large_gatherings_ban,
        "RESTAURANT_LIMITS" as restaurant_limits,
        "BAR_CLOSURES" as bar_closures,
        "FACE_COVERING_REQUIREMENT" as face_covering_requirement,
        "PRIMARY_ELECTION_POSTPONEMENT" as primary_election_postponement,
        "EMERGENCY_DECLARATION" as emergency_declaration,
        "LAST_UPDATED_DATE" as last_updated_date

    from raw_source

)

select * from final
