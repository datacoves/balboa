with raw_source as (

    select
        *
    from {{ source('account_usage', 'pipe_usage_history') }}

),

final as (

    select
        "PIPE_ID" as pipe_id,
        "PIPE_NAME" as pipe_name,
        "START_TIME" as start_time,
        "END_TIME" as end_time,
        "CREDITS_USED" as credits_used,
        "BYTES_INSERTED" as bytes_inserted,
        "FILES_INSERTED" as files_inserted

    from raw_source

)

select * from final
