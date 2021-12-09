with raw_source as (

    select
        parse_json(replace(_airbyte_data::string,'"NaN"', 'null')) as airbyte_data_clean,
        *
    from {{ source('public', 'metadata') }}

),

final as (

    select
        "TABLE" as table,
        "DESCRIPTION" as description,
        "COLUMN" as column,
        "TYPE" as type,
        "NULLABLE" as nullable,
        "COMMENTS" as comments,
        "SOURCE" as source

    from raw_source

)

select * from final
