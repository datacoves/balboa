
  create or replace  view staging_BALBOA.source_dbt_artifacts.stg_dbt__artifacts 
  
  copy grants as (
    with base as (

    select *
    from balboa.source_dbt_artifacts.artifacts

),

fields as (

    select
        data:metadata:invocation_id::string as command_invocation_id,
        data:metadata:env:DBT_CLOUD_RUN_ID::int as dbt_cloud_run_id,
        generated_at,
        path,
        artifact_type,
        data
    from base

),

artifacts as (

    select
        command_invocation_id,
        dbt_cloud_run_id,
        
    sha2_hex(coalesce(dbt_cloud_run_id::string, command_invocation_id::string), 256)
 as artifact_run_id,
        generated_at,
        path,
        artifact_type,
        data
    from fields

)

select * from artifacts
  );
