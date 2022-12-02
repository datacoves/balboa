with raw_source as (

    select
        parse_json(replace(_airbyte_data::string, '"NaN"', 'null')) as airbyte_data_clean,
        *
    from {{ source('RAW', '_AIRBYTE_RAW_COUNTRY_POPULATIONS') }}

),

final as (

    select
        _airbyte_data:"Country Code"::varchar as country_code,
        _airbyte_data:"Country Name"::varchar as country_name,
        _airbyte_data:"Value"::varchar as value,
        _airbyte_data:"Year"::varchar as year,
        "_AIRBYTE_AB_ID"::varchar as airbyte_ab_id,
        "_AIRBYTE_DATA"::variant as airbyte_data,
        "_AIRBYTE_EMITTED_AT"::timestamp_tz as airbyte_emitted_at

    from raw_source

)

select * from final
order by country_code
