with raw_source as (

    select
        parse_json(replace(_airbyte_data::string,'"NaN"', 'null')) as airbyte_data_clean,
        *
    from {{ source('public', 'hs_bulk_data') }}

),

final as (

    select
        "LONG" as long,
        "LAT" as lat,
        "HEALTHCARE_PROVIDER_TYPE" as healthcare_provider_type,
        "NAME" as name,
        "OPERATOR" as operator,
        "BEDS" as beds,
        "STAFF_MEDICAL" as staff_medical,
        "STAFF_NURSING" as staff_nursing

    from raw_source

)

select * from final
