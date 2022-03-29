

with dbt_exposures as (

    select * from 
    
        BALBOA.source_dbt_artifacts.stg_dbt__exposures
    


),

run_results as (

    select *
    from 
    
        BALBOA.source_dbt_artifacts.fct_dbt__run_results
    


),

dbt_exposures_incremental as (

    select dbt_exposures.*
    from dbt_exposures
    -- Inner join with run results to enforce consistency and avoid race conditions.
    -- https://github.com/brooklyn-data/dbt_artifacts/issues/75
    inner join run_results on
        dbt_exposures.artifact_run_id = run_results.artifact_run_id

    
        -- this filter will only be applied on an incremental run
        where dbt_exposures.artifact_generated_at > (select max(artifact_generated_at) from BALBOA.source_dbt_artifacts.dim_dbt__exposures)
    

),

fields as (

    select
        t.manifest_exposure_id,
        t.command_invocation_id,
        t.dbt_cloud_run_id,
        t.artifact_run_id,
        t.artifact_generated_at,
        t.node_id,
        t.name,
        t.type,
        t.owner,
        t.maturity,
        f.value::string as output_feeds,
        t.package_name
    from dbt_exposures_incremental as t,
        lateral flatten(input => depends_on_nodes) as f

)

select * from fields