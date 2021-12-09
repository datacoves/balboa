with raw_source as (

    select
        parse_json(replace(_airbyte_data::string,'"NaN"', 'null')) as airbyte_data_clean,
        *
    from {{ source('public', 'hum_restrictions_country') }}

),

final as (

    select
        "COUNTRY" as country,
        "ISO3166_1" as iso3166_1,
        "LONG" as long,
        "LAT" as lat,
        "PUBLISHED" as published,
        "SOURCES" as sources,
        "RESTRICTION_TEXT" as restriction_text,
        "INFO_DATE" as info_date,
        "QUARANTINE_TEXT" as quarantine_text,
        "LAST_UPDATE_DATE" as last_update_date

    from raw_source

)

select * from final
