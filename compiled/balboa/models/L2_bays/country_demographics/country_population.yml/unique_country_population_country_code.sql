
    
    

select
    country_code as unique_field,
    count(*) as n_records

from BALBOA.l2_country_demographics.country_population
where country_code is not null
group by country_code
having count(*) > 1


