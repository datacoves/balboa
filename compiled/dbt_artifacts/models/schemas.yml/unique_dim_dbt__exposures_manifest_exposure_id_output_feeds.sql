
    
    

select
    manifest_exposure_id || '-' || output_feeds as unique_field,
    count(*) as n_records

from 
    
        BALBOA.source_dbt_artifacts.dim_dbt__exposures
    

where manifest_exposure_id || '-' || output_feeds is not null
group by manifest_exposure_id || '-' || output_feeds
having count(*) > 1


