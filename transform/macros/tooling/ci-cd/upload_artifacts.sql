{# Macro for uploading dbt artifacts to a snowflake stage. #}
{#
    dbt run-operation upload_artifacts --args "{stage: 'RAW.DBT_ARTIFACTS.ARTIFACTS', artifacts: ['manifest.json', 'run_results.json']}"
#}

{% macro upload_artifacts(stage = 'RAW.DBT_ARTIFACTS.ARTIFACTS', artifacts = ['manifest.json', 'run_results.json', 'catalog.json'], run_as_role = 'transformer_dbt') %}
    {{ print("Uploading manifest for dbt version: " ~ dbt_version) }}

    {% set set_date_query %}
        SELECT TO_CHAR(SYSTIMESTAMP(), 'YYYYMMDD_HH24MISS') as timestamp, TO_CHAR(SYSTIMESTAMP(), 'YYYYMM') as day_month;
    {% endset %}
    {% set results = run_query(set_date_query) %}

    {% set target_dir = "target/" %}

    {% set file_prefix = results.columns[0].values()[0] ~ "_" %}
    {% set archive_folder_name = "archive/" ~ results.columns[1].values()[0] ~ "/" %}

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

    {% for artifact in artifacts %}

        {% set artifact_path =  "file://" + target_dir ~ artifact %}

        {% set artifact_file_name = file_prefix ~ artifact %}
        {% set artifact_archive_path =  archive_folder_name ~ artifact_file_name %}

        {% set put_query %}
            put {{ artifact_path }} @{{ stage }} AUTO_COMPRESS=false OVERWRITE=true;
            put {{ artifact_path }} @{{ stage }}/{{artifact_archive_path}} AUTO_COMPRESS=false OVERWRITE=true;
        {% endset %}

        {{ print("\nSnowflake PUT query for file: " ~ artifact )}}
        {# This is just here to format the string and remove the leading spaces #}
        {% for line in put_query.splitlines() %}
            {{ print(line.lstrip()) }}
        {% endfor %}

        {% set results = run_query(put_query) %}

    {% endfor %}

    {{ print("="*85) }}
    {{ print("\nItems in stage " ~ stage ~ " after PUT") }}
    {% set results = run_query(list_stage_query) %}
    {{ results.exclude('md5').print_table(max_column_width=40) }}

{% endmacro %}
