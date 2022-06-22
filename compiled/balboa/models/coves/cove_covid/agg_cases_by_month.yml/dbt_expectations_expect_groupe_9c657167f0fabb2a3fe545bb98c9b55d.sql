

 
with latest_grouped_timestamps as (

    select
        state,
        max(1) as join_key,
        max(cast(date as 
    timestamp_ntz
)) as latest_timestamp_column
    from
        BALBOA.cove_covid.agg_cases_by_month
    where
        -- to exclude erroneous future dates
        cast(date as 
    timestamp_ntz
) <= cast(convert_timezone('UTC', 'America/Los_Angeles', 
    current_timestamp::
    timestamp_ntz

) as 
    timestamp_ntz
)
        

    group by 1

),
total_row_counts as (

    select
        max(1) as join_key,
        count(*) as row_count
    from
        latest_grouped_timestamps

),
outdated_grouped_timestamps as (

    select *
    from
        latest_grouped_timestamps
    where
        -- are the max timestamps per group older than the specified cutoff?
        latest_timestamp_column <
            cast(
                

    dateadd(
        day,
        -180,
        cast(convert_timezone('UTC', 'America/Los_Angeles', 
    current_timestamp::
    timestamp_ntz

) as 
    timestamp_ntz
)
        )


                as 
    timestamp_ntz

            )

),
validation_errors as (

    select
        r.row_count,
        t.*
    from
        total_row_counts r
        left join
        outdated_grouped_timestamps t
        on r.join_key = t.join_key
    where
        -- fail if either no rows were returned due to row_condition,
        -- or the recency test returned failed rows
        r.row_count = 0
        or
        t.join_key is not null

)
select * from validation_errors


