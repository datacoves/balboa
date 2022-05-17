with base as (

    select *
    from BALBOA.source_dbt_artifacts.stg_dbt__artifacts

),

base_nodes as (

    select *
    from BALBOA.source_dbt_artifacts.stg_dbt__nodes

),

base_v2 as (

    select *
    from BALBOA.source_dbt_artifacts.dbt_run_results_nodes

),

run_results as (

    select *
    from base
    where artifact_type = 'run_results.json'

),

fields as (

    -- V1 uploads
    

    select
        run_results.command_invocation_id,
        run_results.dbt_cloud_run_id,
        run_results.artifact_run_id,
        run_results.generated_at::timestamp_tz as artifact_generated_at,
        run_results.data:args:which::string as execution_command,
        coalesce(run_results.data:args:full_refresh, 'false')::boolean as was_full_refresh,
        result.value:unique_id::string as node_id,
        result.value:status::string as status,

        -- The first item in the timing array is the model-level `compile`
        result.value:timing[0]:started_at::timestamp_tz as compile_started_at,

        -- The second item in the timing array is `execute`.
        result.value:timing[1]:completed_at::timestamp_tz as query_completed_at,

        -- Confusingly, this does not match the delta of the above two timestamps.
        -- should we calculate it instead?
        coalesce(result.value:execution_time::float, 0) as total_node_runtime,

        -- Include the raw JSON to unpack the rest later.
        result.value as result_json
    from run_results as run_results,
        lateral flatten(input => run_results.data:results) as result



    union all

    -- V2 uploads
    -- NB: We can safely select * because we know the schemas are the same
    -- as they're made by the same macro.
    select * from base_v2

),

surrogate_key as (

    select
        md5(cast(coalesce(cast(fields.command_invocation_id as 
    varchar
), '') || '-' || coalesce(cast(fields.node_id as 
    varchar
), '') as 
    varchar
)) as node_execution_id,
        fields.command_invocation_id,
        fields.dbt_cloud_run_id,
        fields.artifact_run_id,
        fields.artifact_generated_at,
        fields.was_full_refresh,
        fields.node_id,
        base_nodes.resource_type,
        split(fields.result_json:thread_id::string, '-')[1]::integer as thread_id,
        fields.status,
        fields.result_json:message::string as message,
        fields.compile_started_at,
        fields.query_completed_at,
        fields.total_node_runtime,
        fields.result_json:adapter_response:rows_affected::int as rows_affected,
        fields.result_json
    from fields
    -- Inner join so that we only represent results for nodes which definitely have a manifest
    -- and visa versa.
    inner join base_nodes on (
        fields.artifact_run_id = base_nodes.artifact_run_id
        and fields.node_id = base_nodes.node_id)

)

select * from surrogate_key