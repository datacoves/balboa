with raw_source as (

    select
        parse_json(replace(_airbyte_data::string,'"NaN"', 'null')) as airbyte_data_clean,
        *
    from {{ source('public', 'cdc_inpatient_beds_all') }}

),

final as (

    select
        "STATE" as state,
        "DATE" as date,
        "INPATIENT_BEDS_OCCUPIED" as inpatient_beds_occupied,
        "INPATIENT_BEDS_LOWER_BOUND" as inpatient_beds_lower_bound,
        "INPATIENT_BEDS_UPPER_BOUND" as inpatient_beds_upper_bound,
        "INPATIENT_BEDS_IN_USE_PCT" as inpatient_beds_in_use_pct,
        "INPATIENT_BEDS_IN_USE_PCT_LOWER_BOUND" as inpatient_beds_in_use_pct_lower_bound,
        "INPATIENT_BEDS_IN_USE_PCT_UPPER_BOUND" as inpatient_beds_in_use_pct_upper_bound,
        "TOTAL_INPATIENT_BEDS" as total_inpatient_beds,
        "TOTAL_INPATIENT_BEDS_LOWER_BOUND" as total_inpatient_beds_lower_bound,
        "TOTAL_INPATIENT_BEDS_UPPER_BOUND" as total_inpatient_beds_upper_bound,
        "ISO3166_1" as iso3166_1,
        "ISO3166_2" as iso3166_2,
        "LAST_REPORTED_FLAG" as last_reported_flag

    from raw_source

)

select * from final
