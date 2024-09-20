





with validation_errors as (

    select
        country_code, year
    from BALBOA.L1_COUNTRY_DATA.country_populations_v2
    group by country_code, year
    having count(*) > 1

)

select *
from validation_errors


