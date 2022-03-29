with raw_source as (

    select * from raw.raw._AIRBYTE_RAW_NYT_COVID_DATA

),

final as (

    select
        _airbyte_data:cases::integer as cases,
        _airbyte_data:deaths::integer as deaths,
        _airbyte_data:date::timestamp_ntz as date,
        _airbyte_data:fips::integer as fips,
        _airbyte_data:state::varchar as state,
        _airbyte_ab_id,
        _airbyte_emitted_at

    from raw_source

)

select * from final