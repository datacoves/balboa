{{ config(
    materialized = 'dynamic_table',
    snowflake_warehouse = 'wh_transforming_dynamic_tables',
    target_lag = '1 minute',
) }}

select
    personal_loans.addr_state as state,
    state_codes.state_name,
    count(*) as number_of_loans

from {{ ref("stg_personal_loans") }} as personal_loans
join {{ ref("state_codes") }} as state_codes
    on personal_loans.addr_state = state_codes.state_code
group by 1, 2
order by 2 desc
limit 10
