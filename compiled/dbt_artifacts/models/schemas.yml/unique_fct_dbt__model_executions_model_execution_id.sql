
    
    

select
    model_execution_id as unique_field,
    count(*) as n_records

from 
    
        BALBOA.source_dbt_artifacts.fct_dbt__model_executions
    

where model_execution_id is not null
group by model_execution_id
having count(*) > 1


