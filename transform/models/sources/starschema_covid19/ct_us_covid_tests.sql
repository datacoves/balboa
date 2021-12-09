with raw_source as (

    select
        parse_json(replace(_airbyte_data::string,'"NaN"', 'null')) as airbyte_data_clean,
        *
    from {{ source('public', 'ct_us_covid_tests') }}

),

final as (

    select
        "COUNTRY_REGION" as country_region,
        "PROVINCE_STATE" as province_state,
        "DATE" as date,
        "POSITIVE" as positive,
        "POSITIVE_SINCE_PREVIOUS_DAY" as positive_since_previous_day,
        "NEGATIVE" as negative,
        "NEGATIVE_SINCE_PREVIOUS_DAY" as negative_since_previous_day,
        "PENDING" as pending,
        "PENDING_SINCE_PREVIOUS_DAY" as pending_since_previous_day,
        "DEATH" as death,
        "DEATH_SINCE_PREVIOUS_DAY" as death_since_previous_day,
        "HOSPITALIZED" as hospitalized,
        "HOSPITALIZED_SINCE_PREVIOUS_DAY" as hospitalized_since_previous_day,
        "TOTAL" as total,
        "TOTAL_SINCE_PREVIOUS_DAY" as total_since_previous_day,
        "ISO3166_1" as iso3166_1,
        "ISO3166_2" as iso3166_2,
        "LAST_UPDATED_DATE" as last_updated_date,
        "LAST_REPORTED_FLAG" as last_reported_flag,
        "HOSPITALIZEDCURRENTLY" as hospitalizedcurrently,
        "HOSPITALIZEDCURRENTLYINCREASE" as hospitalizedcurrentlyincrease,
        "HOSPITALIZEDCUMULATIVE" as hospitalizedcumulative,
        "HOSPITALIZEDCUMULATIVEINCREASE" as hospitalizedcumulativeincrease,
        "INICUCURRENTLY" as inicucurrently,
        "INICUCURRENTLYINCREASE" as inicucurrentlyincrease,
        "INICUCUMULATIVE" as inicucumulative,
        "INICUCUMULATIVEINCREASE" as inicucumulativeincrease,
        "ONVENTILATORCURRENTLY" as onventilatorcurrently,
        "ONVENTILATORCURRENTLYINCREASE" as onventilatorcurrentlyincrease,
        "ONVENTILATORCUMULATIVE" as onventilatorcumulative,
        "ONVENTILATORCUMULATIVEINCREASE" as onventilatorcumulativeincrease

    from raw_source

)

select * from final
