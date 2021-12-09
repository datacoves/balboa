with raw_source as (

    select
        parse_json(replace(_airbyte_data::string, '"NaN"', 'null')) as airbyte_data_clean,
        *
    from {{ source('raw', '_airbyte_raw_country_populations') }}

),

final as (

    select
        airbyte_data_clean:"Country Code"::varchar as country_code,
        airbyte_data_clean:"Country Name"::varchar as country_name,
        airbyte_data_clean:"Value"::varchar as value,
        airbyte_data_clean:"Year"::varchar as year,
        "_AIRBYTE_AB_ID" as _airbyte_ab_id,
        "_AIRBYTE_EMITTED_AT" as _airbyte_emitted_at

    from raw_source

)

select * from final
order by 1
