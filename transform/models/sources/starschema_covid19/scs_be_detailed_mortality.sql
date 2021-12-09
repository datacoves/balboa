with raw_source as (

    select
        parse_json(replace(_airbyte_data::string,'"NaN"', 'null')) as airbyte_data_clean,
        *
    from {{ source('public', 'scs_be_detailed_mortality') }}

),

final as (

    select
        "REGION" as region,
        "SEX" as sex,
        "AGEGROUP" as agegroup,
        "DATE" as date,
        "DEATHS" as deaths,
        "ISO3166_1" as iso3166_1,
        "ISO3166_2" as iso3166_2,
        "LAST_UPDATED_DATE" as last_updated_date

    from raw_source

)

select * from final
