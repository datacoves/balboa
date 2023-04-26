with raw_source as (

    select *
    from {{ source('GOOGLE_ANALYTICS_4', 'ENGAGEMENT_EVENTS_REPORT') }}


),

final as (

    select
        "DATE"::date as date,
        "PROPERTY"::varchar as property,
        "_FIVETRAN_ID"::varchar as fivetran_id,
        "EVENT_NAME"::varchar as event_name,
        "TOTAL_USERS"::number as total_users,
        "EVENT_COUNT_PER_USER"::float as event_count_per_user,
        "EVENT_COUNT"::number as event_count,
        "TOTAL_REVENUE"::number as total_revenue,
        "_FIVETRAN_SYNCED"::timestamp_tz as fivetran_synced

    from raw_source

)

select * from final
