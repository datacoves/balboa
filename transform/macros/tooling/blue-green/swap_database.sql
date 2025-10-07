{# This macro swaps two databases, for use in blue/green runs. #}
{#
    To run:
    dbt run-operation swap_database --args "{db1: dev_commercial_dw2, db2: dev_commercial_dw}"

    To create missing db2 if it doesn't exist:
    dbt run-operation swap_database --args "{db1: dev_commercial_dw2, db2: dev_commercial_dw, create_missing_db: true}"
#}

{%- macro swap_database(db1, db2, create_missing_db=false) -%}
    {# Check if db2 exists #}
    {% set check_db2_sql %}
        select count(*) as db_count
        from information_schema.databases
        where database_name = upper('{{ db2 }}')
    {% endset %}

    {% set db2_exists_result = run_query(check_db2_sql) %}
    {% set db2_exists = db2_exists_result.columns[0].values()[0] > 0 %}

    {# Create db2 if it doesn't exist and create_missing_db is true #}
    {% if not db2_exists %}
        {% if create_missing_db %}
            {% set create_db2_sql %}
                create database {{ db2 }};
            {% endset %}

            {% do run_query(create_db2_sql) %}
            {{ print("Created database " ~ db2 ~ " as it did not exist") }}
        {% else %}
            {{ exceptions.raise_compiler_error("Database " ~ db2 ~ " does not exist. Set create_missing_db=true to create it automatically.") }}
        {% endif %}
    {% endif %}

    {# Perform the swap #}
    {% set swap_db_sql %}
        alter database {{ db1 }} swap with {{ db2 }};
    {% endset %}

    {% do run_query(swap_db_sql) %}
    {{ log("Swapped database " ~ db1 ~ " with " ~ db2, info=true) }}

{%- endmacro -%}
