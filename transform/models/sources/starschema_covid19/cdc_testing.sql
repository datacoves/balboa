with raw_source as (

    select
        parse_json(replace(_airbyte_data::string,'"NaN"', 'null')) as airbyte_data_clean,
        *
    from {{ source('public', 'cdc_testing') }}

),

final as (

    select
        "ISO3166_1" as iso3166_1,
        "ISO3166_2" as iso3166_2,
        "DATE" as date,
        "POSITIVE" as positive,
        "NEGATIVE" as negative,
        "INCONCLUSIVE" as inconclusive

    from raw_source

)

select * from final
