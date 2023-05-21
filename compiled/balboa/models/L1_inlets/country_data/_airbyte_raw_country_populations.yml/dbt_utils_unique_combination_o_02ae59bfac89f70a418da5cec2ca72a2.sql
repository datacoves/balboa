





with validation_errors as (

    select
        country_code, year
    from BALBOA.l1_country_data._airbyte_raw_country_populations
    group by country_code, year
    having count(*) > 1

)

select *
from validation_errors


