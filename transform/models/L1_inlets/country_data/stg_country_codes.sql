select
    country_name,
    alpha_2_code as iso_2,
    alpha_3_code as iso_3,
    numeric_code
from {{ ref("country_codes") }}

