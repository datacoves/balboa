{# Macro for setting up Snowflake objects. #}
{#
    Run using
    dbt run-operation create_non_permifrost_items  --args {'databases: [raw, balboa, balboa_apps, balboa_dev]'}
    dbt run-operation create_non_permifrost_items  --args {'databases: [raw, balboa, balboa_apps, balboa_dev], accounts_to_share_to: [acct1, acct2], stages: [stg10, stg11]'}

    dbt run-operation create_non_permifrost_items  --args {'databases: [raw, balboa, balboa_apps, balboa_dev], stages: [raw.dbt_artifacts.artifacts, balboa_apps.resources.streamlit]'} -t prd_pii

    RUN ../secure/create_snowflake_objects.py before running this
    ../secure/create_snowflake_objects.py -t prd_pii -s roles
    ../secure/create_snowflake_objects.py -t prd_pii -s warehouses

    AFTER THIS run
    ../secure/create_snowflake_objects.py -t prd_pii
#}

{%- macro create_non_permifrost_items(databases = [], accounts_to_share_to = [], stages = []) -%}

    {%- set create_objects_sql -%}

        use role accountadmin;

        grant create database on account to role transformer_dbt;

        {%- for account in accounts_to_share_to -%}
            alter database {{ database_name }} enable replication to {{ account }};
        {%- endfor -%}

        grant monitor usage on account to role z_db_snowflake;

        {%- for database in databases -%}
            create database if not exists {{ database }};
            grant ownership on database {{ database }} to role transformer_dbt revoke current grants;
        {%- endfor -%}

        grant create masking policy on schema {{ var('common_masking_policy_db') }}.{{ var('common_masking_policy_schema') }} to role transformer_dbt;
        grant apply masking policy on account to role transformer_dbt;

        use role securityadmin;
        grant imported privileges on database snowflake to role z_db_snowflake;

        {%- for stage in stages -%}
            {%- set database_name = stage.split('.')[0] -%}
            {%- set schema_name = stage.split('.')[1] -%}

            use role transformer_dbt;
            create schema if not exists {{ database_name }}.{{ schema_name }};

            create stage if not exists {{ stage }}
            directory = (enable=true)
            file_format = (type=csv field_delimiter=none record_delimiter=none);

            use role securityadmin;

            grant read on stage {{stage}} to role z_stage_{{ schema_name }}_read;
            grant read on stage {{stage}} to role z_stage_{{ schema_name }}_write;
            grant write on stage {{stage}} to role z_stage_{{ schema_name }}_write;

        {%- endfor -%}

        use role securityadmin;
        grant role transformer_dbt to user {{target.user}};
    {%- endset -%}

    {{ print("\n\nRunning the following SQL:") }}
    {{ print("="*30) }}
    {{ print(create_objects_sql) }}
    {{ print("="*30) }}

    {%- set results = run_query(create_objects_sql) -%}

    {{ print(results.columns[0].values()) }}
    {{ print("Objects Created") }}

{%- endmacro -%}
