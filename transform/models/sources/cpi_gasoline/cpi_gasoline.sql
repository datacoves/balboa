with raw_source as (

    select
        *
    from {{ source('cpi_gasoline', 'cpi_gasoline') }}

),

final as (

    select
        _airbyte_data:"       value"::varchar as price,
        _airbyte_data:"footnote_codes"::varchar as footnote_codes,
        _airbyte_data:"period"::varchar as month,
        _airbyte_data:"series_id        "::varchar as series_id,
        _airbyte_data:"year"::varchar as year,
        "_AIRBYTE_AB_ID" as _airbyte_ab_id,
        "_AIRBYTE_EMITTED_AT" as _airbyte_emitted_at

    from raw_source

)

select * from final
