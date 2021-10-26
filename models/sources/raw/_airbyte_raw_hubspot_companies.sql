with raw_source as (

    select
        *
    from {{ source('raw', '_airbyte_raw_hubspot_companies') }}

),

final as (

    select
        "_AIRBYTE_AB_ID" as _airbyte_ab_id,
        "_AIRBYTE_EMITTED_AT" as _airbyte_emitted_at,
        _airbyte_data:"archived"::varchar as archived,
        _airbyte_data:"contacts"::varchar as contacts,
        _airbyte_data:"createdAt"::varchar as createdat,
        _airbyte_data:"id"::varchar as id,
        _airbyte_data:"properties"::varchar as properties,
        _airbyte_data:"updatedAt"::varchar as updatedat

    from raw_source

)

select * from final

