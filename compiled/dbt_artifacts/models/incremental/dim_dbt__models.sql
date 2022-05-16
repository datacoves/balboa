

with dbt_nodes as (

    select * from BALBOA.source_dbt_artifacts.stg_dbt__nodes

),

dbt_models_incremental as (

    select *
    from dbt_nodes
    where resource_type = 'model'

        
            -- this filter will only be applied on an incremental run
            and coalesce(artifact_generated_at > (select max(artifact_generated_at) from BALBOA.source_dbt_artifacts.dim_dbt__models), true)
        

),

fields as (

    select
        manifest_node_id as manifest_model_id,
        command_invocation_id,
        dbt_cloud_run_id,
        artifact_run_id,
        artifact_generated_at,
        node_id,
        node_database as model_database,
        node_schema as model_schema,
        name,
        to_array(node_json:depends_on:nodes) as depends_on_nodes,
        node_json:package_name::string as package_name,
        node_json:path::string as model_path,
        node_json:checksum.checksum::string as checksum,
        node_json:config.materialized::string as model_materialization
    from dbt_models_incremental

)

select * from fields