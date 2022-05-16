

with dbt_nodes as (

    select * from BALBOA.source_dbt_artifacts.stg_dbt__nodes

),

dbt_seeds_incremental as (

    select *
    from dbt_nodes
    where resource_type = 'seed'

        
            -- this filter will only be applied on an incremental run
            and coalesce(artifact_generated_at > (select max(artifact_generated_at) from BALBOA.source_dbt_artifacts.dim_dbt__seeds), true)
        

),

fields as (

    select
        manifest_node_id as manifest_seed_id,
        command_invocation_id,
        dbt_cloud_run_id,
        artifact_run_id,
        artifact_generated_at,
        node_id,
        node_database as seed_database,
        node_schema as seed_schema,
        name,
        to_array(node_json:depends_on:nodes) as depends_on_nodes,
        node_json:package_name::string as package_name,
        node_json:path::string as seed_path,
        node_json:checksum.checksum::string as checksum
    from dbt_seeds_incremental

)

select * from fields