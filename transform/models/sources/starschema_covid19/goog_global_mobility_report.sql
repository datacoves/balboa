with raw_source as (

    select
        parse_json(replace(_airbyte_data::string,'"NaN"', 'null')) as airbyte_data_clean,
        *
    from {{ source('public', 'goog_global_mobility_report') }}

),

final as (

    select
        "COUNTRY_REGION" as country_region,
        "PROVINCE_STATE" as province_state,
        "ISO_3166_1" as iso_3166_1,
        "ISO_3166_2" as iso_3166_2,
        "DATE" as date,
        "GROCERY_AND_PHARMACY_CHANGE_PERC" as grocery_and_pharmacy_change_perc,
        "PARKS_CHANGE_PERC" as parks_change_perc,
        "RESIDENTIAL_CHANGE_PERC" as residential_change_perc,
        "RETAIL_AND_RECREATION_CHANGE_PERC" as retail_and_recreation_change_perc,
        "TRANSIT_STATIONS_CHANGE_PERC" as transit_stations_change_perc,
        "WORKPLACES_CHANGE_PERC" as workplaces_change_perc,
        "LAST_UPDATE_DATE" as last_update_date,
        "LAST_REPORTED_FLAG" as last_reported_flag,
        "SUB_REGION_2" as sub_region_2

    from raw_source

)

select * from final
