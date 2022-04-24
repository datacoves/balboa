
    
    

select
    node_id as unique_field,
    count(*) as n_records

from BALBOA.source_dbt_artifacts.fct_dbt__latest_full_model_executions
where node_id is not null
group by node_id
having count(*) > 1


