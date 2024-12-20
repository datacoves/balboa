{{ config( materialized='view') }}
select
    'no_tests' as test_name,
    0 as count_failed,
    null as error_state,
    null as warning_state,
    parse_json('[]') as tested_models
where 1 = 0
