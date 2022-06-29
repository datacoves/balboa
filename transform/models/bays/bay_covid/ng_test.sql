select
    {{ dbt_utils.star(ref("base_cases")) }}
from {{ ref("base_cases") }}
