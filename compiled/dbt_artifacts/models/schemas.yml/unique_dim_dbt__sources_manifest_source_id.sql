
    
    

select
    manifest_source_id as unique_field,
    count(*) as n_records

from 
    
        BALBOA.source_dbt_artifacts.dim_dbt__sources
    

where manifest_source_id is not null
group by manifest_source_id
having count(*) > 1


