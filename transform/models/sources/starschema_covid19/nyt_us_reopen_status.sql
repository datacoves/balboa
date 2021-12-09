with raw_source as (

    select
        parse_json(replace(_airbyte_data::string,'"NaN"', 'null')) as airbyte_data_clean,
        *
    from {{ source('public', 'nyt_us_reopen_status') }}

),

final as (

    select
        "STATE_CODE" as state_code,
        "STATE" as state,
        "STATUS_DESCRIPTION" as status_description,
        "DATE_DETAILS_DESCRIPTION" as date_details_description,
        "RESTRICTION_START" as restriction_start,
        "RESTRICTION_END" as restriction_end,
        "STATUS_DETAILS" as status_details,
        "EXTERNAL_LINK" as external_link,
        "OPENED_PERSONAL_CARE" as opened_personal_care,
        "CLOSED_OUTDOOR_AND_RECREATION" as closed_outdoor_and_recreation,
        "CLOSED_ENTERTAINMENT" as closed_entertainment,
        "OPENED_OUTDOOR_AND_RECREATION" as opened_outdoor_and_recreation,
        "CLOSED_FOOD_AND_DRINK" as closed_food_and_drink,
        "OPENED_HOUSES_OF_WORSHIP" as opened_houses_of_worship,
        "OPENED_ENTERTAINMENT" as opened_entertainment,
        "OPENED_FOOD_AND_DRINK" as opened_food_and_drink,
        "OPENED_RETAIL" as opened_retail,
        "OPENED_INDUSTRIES" as opened_industries,
        "POPULATION" as population,
        "LAST_UPDATE_DATE" as last_update_date

    from raw_source

)

select * from final
