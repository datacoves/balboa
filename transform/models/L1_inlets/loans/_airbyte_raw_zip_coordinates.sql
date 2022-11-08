with raw_source as (

    select *
    from {{ source('loans', '_airbyte_raw_zip_coordinates') }}

),

final as (

    select
        _airbyte_data:"LAT"::varchar as lat,
        _airbyte_data:"LON"::varchar as lon,
        _airbyte_data:"ZIP"::varchar as zip,
        "_AIRBYTE_AB_ID"::VARCHAR as airbyte_ab_id,
        "_AIRBYTE_DATA"::VARIANT as airbyte_data,
        "_AIRBYTE_EMITTED_AT"::TIMESTAMP_TZ as airbyte_emitted_at

    from raw_source

)

select * from final
