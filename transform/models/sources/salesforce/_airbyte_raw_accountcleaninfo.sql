with raw_source as (

    select
        * as airbyte_data_clean
    from {{ source('salesforce', '_airbyte_raw_accountcleaninfo') }}

),

final as (

    select
        "_AIRBYTE_AB_ID" as _airbyte_ab_id,
        "_AIRBYTE_DATA" as _airbyte_data,
        "_AIRBYTE_EMITTED_AT" as _airbyte_emitted_at

    from raw_source

)

select * from final

