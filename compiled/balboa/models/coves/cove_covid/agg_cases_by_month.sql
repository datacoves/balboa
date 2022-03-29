

WITH 
 
base_cases AS (

SELECT * 
FROM 
    
        BALBOA.bay_covid.base_cases
    

), 
state_codes AS (

SELECT * 
FROM 
    
        BALBOA.seeds.state_codes
    

),



final_monthly_cases as (
    select
        date,
        state,
        cases,
        deaths
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
            from base_cases)
    where row_num = 1
    order by date
)

select *
from final_monthly_cases
-- where date < '2021-09-30 00:00:00.000'