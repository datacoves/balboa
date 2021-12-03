{# Macro for returning dbt manifest from snowflake. #}
{# 
    Run using
    dbt run-operation get_last_manifest | sed -e '1,1d' > logs/manifest.json
#}
{# Once this is completed, deferral and state modifiers are available using --state logs #}

{% macro get_last_manifest() %}

    {% set results = run_query("select data from prd_raw.dbt_artifacts.dbt_artifacts where artifact_type = 'manifest.json' order by generated_at desc limit 1") %}
    {{ log(results.columns[0].values()[0], info=True) }}

{% endmacro %}
