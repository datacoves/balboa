



with run_results as (

    select *
    from BALBOA.source_dbt_artifacts.stg_dbt__run_results

),

incremental_run_results as (

    select *
    from run_results

    

),

fields as (

    select
        artifact_generated_at,
        command_invocation_id,
        dbt_cloud_run_id,
        artifact_run_id,
        dbt_version,
        elapsed_time,
        execution_command,
        selected_models,
        target,
        was_full_refresh

        
    from incremental_run_results

)

select * from fields