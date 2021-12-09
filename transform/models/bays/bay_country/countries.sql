select
    countries.display_name,
    countries.region_name,
    countries.iso4217_currency_name as currency,
    current_population.population
from {{ ref('base_country_codes') }} as countries
left join {{ ref('current_population') }} as current_population
    on
        current_population.country_code = countries.iso3166_1_alpha_3