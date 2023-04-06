

 
with latest_grouped_timestamps as (

    select
        state,
        max(1) as join_key,
        max(cast(date_month as timestamp_ntz)) as latest_timestamp_column
    from
        BALBOA.l3_covid_analytics.agg_cases_by_month
    where
        -- to exclude erroneous future dates
        cast(date_month as timestamp_ntz) <= convert_timezone('UTC', 'America/Los_Angeles',
    cast(convert_timezone('UTC', current_timestamp()) as TIMESTAMP)
)
        

    group by 1
),
total_row_counts as (

    select
        state,
        max(1) as join_key,
        count(*) as row_count
    from
        latest_grouped_timestamps
    group by 1


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
        month,
        -24,
        convert_timezone('UTC', 'America/Los_Angeles',
    cast(convert_timezone('UTC', current_timestamp()) as TIMESTAMP)
)
        )


                as timestamp_ntz
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
        on
            
            r.state = t.state and
            
            r.join_key = t.join_key
    where
        -- fail if either no rows were returned due to row_condition,
        -- or the recency test returned failed rows
        r.row_count = 0
        or
        t.join_key is not null

)
select * from validation_errors


