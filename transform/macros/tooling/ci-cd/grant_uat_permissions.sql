{# This macro grants access to a test database #}
{#
    To run: 
    dbt run-operation grant_uat_permissions --args '{db_name: test_commercial_dw_pr_100}'
#}

{%- macro grant_uat_permissions(db_name) -%}
    {% set db_name = db_name | upper %}

    {% set apply_db_grants_sql %}
        grant usage on database {{ db_name }} to role z_db_tst_commercial_dw;
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
            grant usage on schema {{db_name}}.{{ schema[0] }} to z_schema_{{schema[0]}};
            grant select on all tables in schema {{db_name}}.{{ schema[0] }} to role z_tables_views_general;
            grant select on all views in schema {{db_name}}.{{ schema[0] }} to role z_tables_views_general;
        {% endset %}

        {% do run_query(apply_schema_grants_sql) %}
        {{ log("Applied grants on Schema: " ~ db_name ~ '.' ~ schema[0], info=true) }}
    {% endfor %}

    {{ log("Applied grants on Database: " ~ db_name, info=true) }}

{%- endmacro -%}
