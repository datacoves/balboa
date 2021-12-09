with raw_source as (

    select
        parse_json(replace(_airbyte_data::string,'"NaN"', 'null')) as airbyte_data_clean,
        *
    from {{ source('public', 'jhu_vaccines') }}

),

final as (

    select
        "DATE" as date,
        "PROVINCE_STATE" as province_state,
        "FIPS" as fips,
        "DOSES_ALLOC_TOTAL" as doses_alloc_total,
        "DOSES_ALLOC_MODERNA" as doses_alloc_moderna,
        "DOSES_ALLOC_PFIZER" as doses_alloc_pfizer,
        "DOSES_ALLOC_JOHNSON_AND_JOHNSON" as doses_alloc_johnson_and_johnson,
        "DOSES_ALLOC_UNASSIGNED" as doses_alloc_unassigned,
        "DOSES_ALLOC_UNKNOWN" as doses_alloc_unknown,
        "DOSES_SHIPPED_TOTAL" as doses_shipped_total,
        "DOSES_SHIPPED_MODERNA" as doses_shipped_moderna,
        "DOSES_SHIPPED_PFIZER" as doses_shipped_pfizer,
        "DOSES_SHIPPED_JOHNSON_AND_JOHNSON" as doses_shipped_johnson_and_johnson,
        "DOSES_SHIPPED_UNASSIGNED" as doses_shipped_unassigned,
        "DOSES_SHIPPED_UNKNOWN" as doses_shipped_unknown,
        "DOSES_ADMIN_TOTAL" as doses_admin_total,
        "DOSES_ADMIN_MODERNA" as doses_admin_moderna,
        "DOSES_ADMIN_PFIZER" as doses_admin_pfizer,
        "DOSES_ADMIN_JOHNSON_AND_JOHNSON" as doses_admin_johnson_and_johnson,
        "DOSES_ADMIN_UNASSIGNED" as doses_admin_unassigned,
        "DOSES_ADMIN_UNKNOWN" as doses_admin_unknown,
        "PEOPLE_TOTAL" as people_total,
        "PEOPLE_TOTAL_2ND_DOSE" as people_total_2nd_dose,
        "COUNTRY_REGION" as country_region,
        "LAST_UPDATE_DATE" as last_update_date,
        "LAST_REPORTED_FLAG" as last_reported_flag,
        "STABBR" as stabbr

    from raw_source

)

select * from final
