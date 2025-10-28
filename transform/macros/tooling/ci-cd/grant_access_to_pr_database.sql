{# This macro grants access to a test database #}
{#
    To run:
    dbt run-operation grant_access_to_pr_database
#}

{%- macro grant_access_to_pr_database() -%}
    {% set db_name = target.database %}
    {% set pr_db_usage_role = var("pr_db_usage_role") %}
    {% set schema_usage_role_prefix = var("schema_usage_role_prefix") %}

    {% set apply_db_grants_sql %}
        grant usage on database {{ db_name }} to role {{ pr_db_usage_role }};
        grant select on all tables in database {{ db_name }} to role z_tables_views__select;
        grant select on all views in database {{ db_name }} to role z_tables_views__select;
    {% endset %}

    {% do run_query(apply_db_grants_sql) %}

    {% set schemas_list %}
        select schema_name
        from {{ db_name }}.information_schema.schemata
        where schema_name not in ('INFORMATION_SCHEMA','PUBLIC','DBT_TEST__AUDIT')
    {% endset %}

    {% set schemas = run_query(schemas_list) %}

    {% for schema in schemas %}

        {% set apply_schema_grants_sql %}
            grant usage on schema {{ db_name }}.{{ schema[0] }} to {{ schema_usage_role_prefix }}{{ schema[0] }};
        {% endset %}

        {% do run_query(apply_schema_grants_sql) %}
        {{ log("Applied grants on Schema: " ~ db_name ~ '.' ~ schema[0], info=true) }}
    {% endfor %}

    {{ log("Applied grants on Database: " ~ db_name, info=true) }}

{%- endmacro -%}
