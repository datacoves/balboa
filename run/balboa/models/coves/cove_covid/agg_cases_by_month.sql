

      create or replace transient table staging_BALBOA.cove_covid.agg_cases_by_month copy grants as
      (

with cases as (
    select
        *
    from bay_covid.base_cases
),

states as (
    select
        *
    from seeds.state_codes
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
    ) as __q
    where row_num = 1
    order by date
)

select *
from final_monthly_cases
-- where date < '2021-09-30 00:00:00.000'
      );
    