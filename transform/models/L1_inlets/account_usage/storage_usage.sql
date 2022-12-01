with raw_source as (

    select *
    from {{ source('ACCOUNT_USAGE', 'STORAGE_USAGE') }}

),

final as (

    select
        "USAGE_DATE" as usage_date,
        "STORAGE_BYTES" as storage_bytes,
        "STAGE_BYTES" as stage_bytes,
        "FAILSAFE_BYTES" as failsafe_bytes

    from raw_source

)

select * from final
