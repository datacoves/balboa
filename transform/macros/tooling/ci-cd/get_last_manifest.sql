{# Macro for returning dbt manifest from a snowflake stage. #}
{#
    dbt run-operation get_last_manifest
#}
{# Once this is completed, deferral and state modifiers are available using --state logs #}

{% macro get_last_manifest(stage = 'RAW.DBT_ARTIFACTS.ARTIFACTS', run_as_role = 'transformer_dbt_prd') %}
    {# we will put the manifest.json in the log directory and use the with the --state param in dbt #}
    {% set logs_dir = env_var('DBT_HOME') ~ "/logs/" %}

    {% set set_role_query %}
        use role {{ run_as_role }};
    {% endset %}
    {% do run_query(set_role_query) %}

    {# List only the .json files in the root folder (excludes archive dir) #}
    {% set list_stage_query %}
        LIST @{{ stage }} PATTERN = '^((?!(archive/)).)*.json$';
    {% endset %}

    {{ print("\nCurrent items in stage " ~ stage) }}
    {% set results = run_query(list_stage_query) %}
    {{ results.exclude('md5').print_table(max_column_width=40) }}
    {{ print("\n" ~ "="*85) }}

    {% set manifest_destination =  "file://" + logs_dir %}

    {% set put_query %}
        get @{{ stage }}/manifest.json {{ manifest_destination }}
    {% endset %}

    {{ print("\nSnowflake GET query") }}
    {{ print(put_query.lstrip()) }}

    {% set results = run_query(put_query) %}

{% endmacro %}
