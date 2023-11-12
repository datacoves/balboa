{{ config(
    materialized = 'dynamic_table',
    snowflake_warehouse = 'wh_transforming',
    target_lag = '190 days',
) }}

select
    addr_state as state,
    count(*) as number_of_loans
from
    {{ source('LOANS', 'PERSONAL_LOANS') }}
group by 1
order by 2 desc
limit 10
