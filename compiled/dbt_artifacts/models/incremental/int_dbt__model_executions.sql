

with node_executions as (

    select *
    from BALBOA.source_dbt_artifacts.stg_dbt__node_executions

),

model_executions_incremental as (

    select *
    from node_executions
    where resource_type = 'model'

        

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