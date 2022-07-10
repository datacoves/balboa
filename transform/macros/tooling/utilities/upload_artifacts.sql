{# Macro for uploading dbt upload_artifacts to snowflake. #}
{# 
    Run using
    dbt --no-write-json run-operation upload_artifacts --target prd
#}

{% macro upload_artifacts() %}

    {{ dbt_artifacts.upload_dbt_artifacts(['manifest', 'run_results']) }}

{% endmacro %}
