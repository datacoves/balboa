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

    {# Check if files exist by looking at the LIST results #}
    {% set manifest_found = false %}
    {% set catalog_found = false %}

    {% for row in results.rows %}
        {% if 'manifest.json' in row[0] %}
            {% set manifest_found = true %}
        {% endif %}
        {% if 'catalog.json' in row[0] %}
            {% set catalog_found = true %}
        {% endif %}
    {% endfor %}

    {# Only attempt to get files that exist #}
    {% if manifest_found or catalog_found %}
        {% set get_commands = [] %}
        {% if manifest_found %}
            {% set _ = get_commands.append("get @" ~ stage ~ "/manifest.json " ~ artifacts_destination ~ ";") %}
        {% endif %}
        {% if catalog_found %}
            {% set _ = get_commands.append("get @" ~ stage ~ "/catalog.json " ~ artifacts_destination ~ ";") %}
        {% endif %}

        {% set get_query = get_commands | join('\n        ') %}
        {% set results = run_query(get_query) %}

        {{ print("✓ Successfully retrieved available artifact files") }}
        {% if not manifest_found %}
            {{ print("manifest.json not found in stage") }}
        {% endif %}
        {% if not catalog_found %}
            {{ print("catalog.json not found in stage") }}
        {% endif %}
    {% else %}
        {{ print("File not found - no artifact files (manifest.json or catalog.json) found in stage " ~ stage) }}
    {% endif %}

{% endmacro %}
