

      create or replace transient table staging_BALBOA.cove_covid.agg_cases_by_month copy grants as
      (

with  __dbt__cte__base_cases as (


with raw_source as (

    select * from raw.raw._AIRBYTE_RAW_NYT_COVID_DATA

),

final as (

    select
        _airbyte_data:cases::varchar as cases,
        _airbyte_data:deaths::varchar as deaths,
        _airbyte_data:date::timestamp_ntz as date,
        _airbyte_data:fips::varchar as fips,
        _airbyte_data:state::varchar as state,
        _airbyte_ab_id,
        _airbyte_emitted_at

    from raw_source

)

select * from final
),cases as (
    select *
    from __dbt__cte__base_cases
),

final_monthly_cases as (
    select
        date,
        state,
        cases,
        deaths
    from (
        select
            cases,
            deaths,
            date,
            state,
            row_number() over (
                partition by
                    state,
                    year(date),
                    month(date)
                order by day(date) desc) as row_num
        from cases
    )
    where row_num = 1
    order by date
)

select *
from final_monthly_cases
-- where date < '2021-09-30 00:00:00.000'
      );
    