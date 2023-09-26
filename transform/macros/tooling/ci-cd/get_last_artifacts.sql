{# Macro for returning dbt manifest from a snowflake stage. #}
{#
    dbt run-operation get_last_artifacts
#}
{# Once this is completed, deferral and state modifiers are available using --state logs #}

{% macro get_last_artifacts(stage = 'RAW.DBT_ARTIFACTS.ARTIFACTS') %}
    {# we will put the manifest.json in the log directory and use the with the --state param in dbt #}
    {% set logs_dir = env_var('DATACOVES__DBT_HOME') ~ "/logs/" %}

    {# List only the .json files in the root folder (excludes archive dir) #}
    {% set list_stage_query %}
        LIST @{{ stage }} PATTERN = '^((?!(archive/)).)*.json$';
    {% endset %}

    {{ print("\nCurrent items in stage " ~ stage) }}
    {% set results = run_query(list_stage_query) %}
    {{ results.exclude('md5').print_table(max_column_width=40) }}
    {{ print("\n" ~ "="*85) }}

    {% set artifacts_destination =  "file://" + logs_dir %}

    {% set get_query %}
        get @{{ stage }}/manifest.json {{ artifacts_destination }};
        get @{{ stage }}/catalog.json {{ artifacts_destination }};
    {% endset %}

    {% set results = run_query(get_query) %}

{% endmacro %}
