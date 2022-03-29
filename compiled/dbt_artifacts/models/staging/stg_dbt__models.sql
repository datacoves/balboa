with base as (

    select *
    from 
    
        BALBOA.source_dbt_artifacts.stg_dbt__artifacts
    


),

manifests as (

    select *
    from base
    where artifact_type = 'manifest.json'

),

flatten as (

    select
        manifests.command_invocation_id,
        manifests.dbt_cloud_run_id,
        manifests.artifact_run_id,
        manifests.generated_at as artifact_generated_at,
        node.key as node_id,
        node.value:database::string as model_database,
        node.value:schema::string as model_schema,
        node.value:name::string as name,
        to_array(node.value:depends_on:nodes) as depends_on_nodes,
        node.value:package_name::string as package_name,
        node.value:path::string as model_path,
        node.value:checksum.checksum::string as checksum,
        node.value:config.materialized::string as model_materialization
    from manifests,
        lateral flatten(input => data:nodes) as node
    where node.value:resource_type = 'model'

),

surrogate_key as (

    select
        md5(cast(coalesce(cast(command_invocation_id as 
    varchar
), '') || '-' || coalesce(cast(node_id as 
    varchar
), '') as 
    varchar
)) as manifest_model_id,
        command_invocation_id,
        dbt_cloud_run_id,
        artifact_run_id,
        artifact_generated_at,
        node_id,
        model_database,
        model_schema,
        name,
        depends_on_nodes,
        package_name,
        model_path,
        checksum,
        model_materialization
    from flatten

)

select * from surrogate_key