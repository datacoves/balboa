with raw_source as (

    select
        parse_json(replace(_airbyte_data::string,'"NaN"', 'null')) as airbyte_data_clean,
        *
    from {{ source('public', 'kff_hcp_capacity') }}

),

final as (

    select
        "COUNTRY_REGION" as country_region,
        "PROVINCE_STATE" as province_state,
        "TOTAL_HOSPITAL_BEDS" as total_hospital_beds,
        "HOSPITAL_BEDS_PER_1000_POPULATION" as hospital_beds_per_1000_population,
        "TOTAL_CHCS" as total_chcs,
        "CHC_SERVICE_DELIVERY_SITES" as chc_service_delivery_sites

    from raw_source

)

select * from final
