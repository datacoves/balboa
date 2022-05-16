

with node_executions as (

    select *
    from BALBOA.source_dbt_artifacts.stg_dbt__node_executions

),

model_executions_incremental as (

    select *
    from node_executions
    where resource_type = 'model'

        
            -- this filter will only be applied on an incremental run
            and coalesce(artifact_generated_at > (select max(artifact_generated_at) from BALBOA.source_dbt_artifacts.int_dbt__model_executions), true)
        

),

fields as (

    select
        node_execution_id as model_execution_id,
        command_invocation_id,
        dbt_cloud_run_id,
        artifact_run_id,
        artifact_generated_at,
        was_full_refresh,
        node_id,
        thread_id,
        status,
        compile_started_at,
        query_completed_at,
        total_node_runtime,
        rows_affected
    from model_executions_incremental

)

select * from fields