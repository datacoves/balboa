{# Macro for uploading dbt manifest and catalog to snowflake. #}
{# 
    Run using
    dbt --no-write-json run-operation upload_manifest_catalog
#}

{% macro upload_manifest_catalog() %}

    {{ dbt_artifacts.upload_dbt_artifacts(['manifest', 'run_results']) }}

{% endmacro %}
