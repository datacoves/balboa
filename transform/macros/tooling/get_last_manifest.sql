{# Macro for returning dbt manifest from snowflake. #}
{# 
    Run using 
    dbt run-operation get_last_manifest | sed -ne '/{/,$ p' > logs/manifest.json 
#}
{# Once this is completed, deferral and state modifiers are available using --state logs #}

{% macro get_last_manifest(artifacts_location = 'balboa.source_dbt_artifacts.artifacts') %}

    {% set results = run_query("select data from " ~ artifacts_location ~ " where artifact_type = 'manifest.json' order by generated_at desc limit 1") %}
    {# This newline is here so we can excluse any dbt related warning lines and timestamps when bringing down the manifest from Snowflake #}
    {{ log("\n" + results.columns[0].values()[0], info=True) }}

{% endmacro %}
