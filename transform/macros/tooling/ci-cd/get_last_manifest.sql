{# Macro for returning dbt manifest from snowflake. #}
{# 
    If there is a problem getting manifest, runL
        dbt run-operation get_last_manifest | more
    Ths problem may be in how the manifest.json file is writen.
    For that, try this and check that the output starts with {  "child_map": {
        dbt run-operation get_last_manifest | awk '/{/ { f = 1 } f'  | sed  "1s/.*/{/" | more
   
    Normally, we get the latest manifest for deferral / slim CI running:
        dbt run-operation get_last_manifest | awk '/{/ { f = 1 } f'  | sed  "1s/.*/{/" > logs/manifest.json
#}
{# Once this is completed, deferral and state modifiers are available using --state logs #}

{% macro get_last_manifest() %}

    {% do log("Getting manifest for dbt version: " ~ dbt_version, info=true) %}

    {# Override database for staging since we want artifacts to go there not current prod database #}
    {% set artifacts = source('dbt_artifacts', 'artifacts') %}

    {% if target.database.startswith('staging_') %}
        {% set artifacts = api.Relation.create(database = target.database, schema=artifacts.schema, identifier=artifacts.identifier) %}
    {% endif %}

    {% do log("Getting manifest from: " ~ artifacts, info=true) %}

    {% set manifest_query %}
        select data 
        from  {{ artifacts }}
        where artifact_type = 'manifest.json'
        and data:"metadata":"dbt_version" = '{{ dbt_version }}'
        order by generated_at desc limit 1 
    {% endset %}

    {% set results = run_query(manifest_query) %}

    {{ log(results.columns[0].values()[0], info=True) }}

{% endmacro %}