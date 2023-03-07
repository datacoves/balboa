{# Macro for setting up Snowflake objects. #}
{#
    Run using
    dbt run-operation create_non_permifrost_items  --args {'database_name: my_database, accounts_to_share_to: [acct1, acct2], stages: [stg10, stg11]'}

#}

{% macro create_non_permifrost_items(database_name, accounts_to_share_to, stages) %}

    {% set create_schema_sql %}
        use role accountadmin;
        grant create database on account to role transformer_dbt;

        {% for account in accounts_to_share_to %}
            alter database {{ database_name }} enable replication to {{ account }};
        {% endfor %}

        use role securityadmin;
        grant imported privileges on database snowflake to role z_db_snowflake;
        grant monitor usage on account to role z_db_snowflake;

        {% for stage in stages %}
            create role if not exists z_stage_{{stage}};
            grant usage on stage raw.public.{{stage}} to role z_stage_{{stage}};
        {% endfor %}

    {% endset %}

    {# {% do run_query(create_schema_sql) %} #}
    {{ print("Ran statements: " ~ create_schema_sql) }}

{% endmacro %}
