select
    count_if(query_status like 'FAIL') / count(query_status) * 100 as query_fail_percentage
from {{ref('stg_query_history')}}