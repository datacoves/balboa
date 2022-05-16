
  create or replace  view staging_BALBOA.source_dbt_artifacts.stg_dbt__nodes 
  
  copy grants as (
    with base as (

    select *
    from source_dbt_artifacts.stg_dbt__artifacts

),

base_v2 as (

    select *
    from balboa.source_dbt_artifacts.dbt_manifest_nodes

),

manifests_v1 as (

    select *
    from base
    where artifact_type = 'manifest.json'

),

flattened_v1 as (

    

    select
        manifests.command_invocation_id,
        manifests.dbt_cloud_run_id,
        manifests.artifact_run_id,
        manifests.generated_at::timestamp_tz as artifact_generated_at,
        node.key as node_id,
        node.value:resource_type::string as resource_type,
        node.value:database::string as node_database,
        node.value:schema::string as node_schema,
        node.value:name::string as name,
        -- Include the raw JSON to unpack other values.
        node.value as node_json
    from manifests_v1 as manifests,
        lateral flatten(input => manifests.data:nodes) as node

    union all

    select
        manifests.command_invocation_id,
        manifests.dbt_cloud_run_id,
        manifests.artifact_run_id,
        manifests.generated_at::timestamp_tz as artifact_generated_at,
        exposure.key as node_id,
        'exposure' as resource_type,
        null as node_database,
        null as node_schema,
        exposure.value:name::string as name,
        -- Include the raw JSON to unpack other values.
        exposure.value as node_json
    from manifests_v1 as manifests,
        lateral flatten(input => manifests.data:exposures) as exposure

    union all

    select
        manifests.command_invocation_id,
        manifests.dbt_cloud_run_id,
        manifests.artifact_run_id,
        manifests.generated_at::timestamp_tz as artifact_generated_at,
        source.key as node_id,
        'source' as resource_type,
        source.value:database::string as node_database,
        source.value:schema::string as node_schema,
        source.value:name::string::string as name,
        -- Include the raw JSON to unpack other values.
        source.value as node_json
    from manifests_v1 as manifests,
        lateral flatten(input => manifests.data:sources) as source



),

deduped_v1 as (

    select *
    from flattened_v1
    -- Deduplicate the V1 issue of potential multiple manifest files.
    -- This is a very likely occurance if using dbt-cloud as each artifact upload
    -- will generate a new manifest.
    qualify row_number() over (partition by artifact_run_id, node_id order by artifact_generated_at asc) = 1

),

unioned as (

    -- V1 uploads
    select * from deduped_v1

    union all

    -- V2 uploads
    -- NB: We can safely select * because we know the schemas are the same
    -- as they're made by the same macro.
    select * from base_v2

),

surrogate_key as (

    select
        md5(cast(coalesce(cast(command_invocation_id as 
    varchar
), '') || '-' || coalesce(cast(node_id as 
    varchar
), '') as 
    varchar
)) as manifest_node_id,
        command_invocation_id,
        dbt_cloud_run_id,
        artifact_run_id,
        artifact_generated_at,
        node_id,
        resource_type,
        node_database,
        node_schema,
        node_json:description::string as node_description,
        name,
        node_json
    from unioned

)

select * from surrogate_key
  );
