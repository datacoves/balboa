with cases as (
    select * from {{ ref('base_cases') }}
),

states as (
    select
        state_code,
        state_name
    from {{ ref('state_codes') }}
),

final_monthly_cases as (
    select
        date,
        state,
        cases,
        round(deaths) as deaths
    from(
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
        from cases)
    where row_num = 1
    order by date
)

select *
from final_monthly_cases
-- where date < '2021-09-30 00:00:00.000'
