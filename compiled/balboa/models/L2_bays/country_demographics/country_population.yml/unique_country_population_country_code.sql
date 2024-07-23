
    
    

select
    country_code as unique_field,
    count(*) as n_records

from BALBOA.L2_COUNTRY_DEMOGRAPHICS.country_population
where country_code is not null
group by country_code
having count(*) > 1


