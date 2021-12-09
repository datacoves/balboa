with raw_source as (

    select
        parse_json(replace(_airbyte_data::string,'"NaN"', 'null')) as airbyte_data_clean,
        *
    from {{ source('public', 'kff_us_policy_actions') }}

),

final as (

    select
        "COUNTRY_REGION" as country_region,
        "PROVINCE_STATE" as province_state,
        "WAIVE_COST_SHARING_FOR_COVID_19_TREATMENT" as waive_cost_sharing_for_covid_19_treatment,
        "FREE_COST_VACCINE_WHEN_AVAILABLE" as free_cost_vaccine_when_available,
        "STATE_REQUIRES_WAIVER_OF_PRIOR_AUTHORIZATION_REQUIREMENTS" as state_requires_waiver_of_prior_authorization_requirements,
        "EARLY_PRESCRIPTION_REFILLS" as early_prescription_refills,
        "MARKETPLACE_SPECIAL_ENROLLMENT_PERIOD" as marketplace_special_enrollment_period,
        "SECTION_1135_WAIVER" as section_1135_waiver,
        "PAID_SICK_LEAVE" as paid_sick_leave,
        "PREMIUM_PAYMENT_GRACE_PERIOD" as premium_payment_grace_period,
        "NOTES" as notes,
        "LAST_UPDATED_DATE" as last_updated_date

    from raw_source

)

select * from final
