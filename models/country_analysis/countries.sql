select
    balboa.dataops_training.base_country_codes.display_name,
    balboa.dataops_training.base_country_codes.region_name,
    balboa.dataops_training.base_country_codes.iso4217_currency_name as currency,
    balboa.dataops_training.current_population.population
from {{ ref('base_country_codes') }}
left join {{ ref('current_population') }}
    on
        balboa.dataops_training.current_population.country_code = balboa.dataops_training.base_country_codes.iso3166_1_alpha_3
