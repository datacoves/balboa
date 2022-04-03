SELECT
    count(query_id) as queries,
    user_name
from {{ref('stg_query_history')}}
group by user_name