

with dbt_nodes as (

    select * from BALBOA.source_dbt_artifacts.stg_dbt__nodes

),

dbt_sources_incremental as (

    select *
    from dbt_nodes
    where resource_type = 'source'

        

),

fields as (

    select
        manifest_node_id as manifest_source_id,
        command_invocation_id,
        dbt_cloud_run_id,
        artifact_run_id,
        artifact_generated_at,
        node_id,
        node_database,
        node_description,
        name,
        node_json:source_name::string as source_name,
        node_json:loader::string as source_loader,
        node_schema as source_schema,
        node_json:package_name::string as package_name,
        node_json:relation_name::string as relation_name,
        node_json:path::string as source_path
    from dbt_sources_incremental

)

select * from fields