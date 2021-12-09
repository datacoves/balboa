with raw_source as (

    select
        parse_json(replace(_airbyte_data::string,'"NaN"', 'null')) as airbyte_data_clean,
        *
    from {{ source('public', 'ihme_covid_19') }}

),

final as (

    select
        "DATE" as date,
        "ALLBED_MEAN" as allbed_mean,
        "ALLBED_LOWER" as allbed_lower,
        "ALLBED_UPPER" as allbed_upper,
        "ICUBED_MEAN" as icubed_mean,
        "ICUBED_LOWER" as icubed_lower,
        "ICUBED_UPPER" as icubed_upper,
        "INVVEN_MEAN" as invven_mean,
        "INVVEN_LOWER" as invven_lower,
        "INVVEN_UPPER" as invven_upper,
        "DEATHS_MEAN" as deaths_mean,
        "DEATHS_LOWER" as deaths_lower,
        "DEATHS_UPPER" as deaths_upper,
        "ADMIS_MEAN" as admis_mean,
        "ADMIS_LOWER" as admis_lower,
        "ADMIS_UPPER" as admis_upper,
        "NEWICU_MEAN" as newicu_mean,
        "NEWICU_LOWER" as newicu_lower,
        "NEWICU_UPPER" as newicu_upper,
        "TOTDEA_MEAN" as totdea_mean,
        "TOTDEA_LOWER" as totdea_lower,
        "TOTDEA_UPPER" as totdea_upper,
        "BEDOVER_MEAN" as bedover_mean,
        "BEDOVER_LOWER" as bedover_lower,
        "BEDOVER_UPPER" as bedover_upper,
        "ICUOVER_MEAN" as icuover_mean,
        "ICUOVER_LOWER" as icuover_lower,
        "ICUOVER_UPPER" as icuover_upper,
        "LAST_UPDATED_DATE" as last_updated_date,
        "LAST_REPORTED_FLAG" as last_reported_flag,
        "COUNTRY_REGION" as country_region,
        "ISO_3166_1" as iso_3166_1,
        "ISO_3166_2" as iso_3166_2,
        "PROVINCE_STATE" as province_state

    from raw_source

)

select * from final
