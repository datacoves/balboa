
    
    

select
    command_invocation_id as unique_field,
    count(*) as n_records

from 
    
        BALBOA.source_dbt_artifacts.fct_dbt__run_results
    

where command_invocation_id is not null
group by command_invocation_id
having count(*) > 1


