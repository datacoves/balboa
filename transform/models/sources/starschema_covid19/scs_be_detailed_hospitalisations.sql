with raw_source as (

    select
        parse_json(replace(_airbyte_data::string,'"NaN"', 'null')) as airbyte_data_clean,
        *
    from {{ source('public', 'scs_be_detailed_hospitalisations') }}

),

final as (

    select
        "PROVINCE" as province,
        "REGION" as region,
        "DATE" as date,
        "NR_REPORTING" as nr_reporting,
        "TOTAL_IN" as total_in,
        "TOTAL_IN_ICU" as total_in_icu,
        "TOTAL_IN_RESP" as total_in_resp,
        "TOTAL_IN_ECMO" as total_in_ecmo,
        "NEW_IN" as new_in,
        "NEW_OUT" as new_out,
        "ISO3166_1" as iso3166_1,
        "ISO3166_2" as iso3166_2,
        "ISO3166_3" as iso3166_3,
        "LAST_UPDATED_DATE" as last_updated_date

    from raw_source

)

select * from final
