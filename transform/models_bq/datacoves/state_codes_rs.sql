with raw_source as (

    select
        {# p∑arse_json(replace(_airbyte_data::string,'"NaN"', 'null')) as airbyte_data_clean, #}
        *
    from {{ source('datacoves', 'state_codes') }}

),

final as (

    select
        "state_name" as state_name,
        "state_code" as state_code

    from raw_source

)

select * from final
{# limit 10 #}
