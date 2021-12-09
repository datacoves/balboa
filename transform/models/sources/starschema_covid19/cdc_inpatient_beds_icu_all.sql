with raw_source as (

    select
        parse_json(replace(_airbyte_data::string,'"NaN"', 'null')) as airbyte_data_clean,
        *
    from {{ source('public', 'cdc_inpatient_beds_icu_all') }}

),

final as (

    select
        "STATE" as state,
        "DATE" as date,
        "STAFFED_ADULT_ICU_BEDS_OCCUPIED" as staffed_adult_icu_beds_occupied,
        "STAFFED_ADULT_ICU_BEDS_OCCUPIED_LOWER_BOUND" as staffed_adult_icu_beds_occupied_lower_bound,
        "STAFFED_ADULT_ICU_BEDS_OCCUPIED_UPPER_BOUND" as staffed_adult_icu_beds_occupied_upper_bound,
        "STAFFED_ADULT_ICU_BEDS_OCCUPIED_PCT" as staffed_adult_icu_beds_occupied_pct,
        "STAFFED_ADULT_ICU_BEDS_OCCUPIED_PCT_LOWER_BOUND" as staffed_adult_icu_beds_occupied_pct_lower_bound,
        "STAFFED_ADULT_ICU_BEDS_OCCUPIED_PCT_UPPER_BOUND" as staffed_adult_icu_beds_occupied_pct_upper_bound,
        "TOTAL_STAFFED_ICU_BEDS" as total_staffed_icu_beds,
        "TOTAL_STAFFED_ICU_BEDS_LOWER_BOUND" as total_staffed_icu_beds_lower_bound,
        "TOTAL_STAFFED_ICU_BEDS_UPPER_BOUND" as total_staffed_icu_beds_upper_bound,
        "ISO3166_1" as iso3166_1,
        "ISO3166_2" as iso3166_2,
        "LAST_REPORTED_FLAG" as last_reported_flag

    from raw_source

)

select * from final
