{# This macro checks if a database was created today #}
{#
    To run:
    dbt run-operation check_database_created_today --args "{db_name: my_database}"

    Returns: prints "true" if database was created today, "false" otherwise
#}

{%- macro check_database_created_today(db_name) -%}
    {% set ns = namespace(result="false") %}

    {% if execute %}

        {# Check if database exists and get creation date #}
        {% set check_db_sql %}
            select
                count(*) as db_count,
                max(created) as creation_date
            from snowflake.information_schema.databases
            where database_name = upper('{{ db_name }}')
        {% endset %}

        {% set db_result = run_query(check_db_sql) %}
        {% set db_exists = db_result.columns[0].values()[0] > 0 %}

        {% if db_exists %}
            {% set creation_date = db_result.columns[1].values()[0] %}

            {# Get today's date #}
            {% set today_sql %}
                select current_date() as today
            {% endset %}

            {% set today_result = run_query(today_sql) %}
            {% set today = today_result.columns[0].values()[0] %}

            {# Compare creation date with today #}
            {% if creation_date %}
                {% set creation_date_only = creation_date.date() %}
                {% set created_today = creation_date_only == today %}

                {% if created_today %}
                    {% set ns.result = "true" %}
                {% endif %}
            {% endif %}
        {% endif %}
    {% endif %}

    {{ print(ns.result) }}

{%- endmacro -%}
