select
    {{ dbt_utils.star(ref('_airbyte_raw_country_populations')) }}
from {{ ref('_airbyte_raw_country_populations') }}
