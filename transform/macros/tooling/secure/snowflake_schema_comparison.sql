{# Macro for finding differences in roles between Permifrost and Snowflake #}
{# 
    Run using
    dbt run-operation snowflake_schema_comparison  --args '{permifrost_schema_list: demo_schema1,demo_schema2, dry_run: true}'
#}

{% macro snowflake_schema_comparison(permifrost_schema_list, dry_run = true) %}
    {{ print("Running as user: " ~ target.user )}}
    {{ print("Running as using target: " ~ target.name )}}
    {{ print('\n')}}

    {% if(permifrost_schema_list |length == 0) %}
        {{ exceptions.raise_compiler_error("List of schemas to compare is empty") }}
    {% else %}}

        {% set permifrost_schemas = permifrost_schema_list.upper().split(',') %}

        {% set permifrost_databases = [] %}

        {% set snowflake_schemas = [] %}

        {% set schemas_to_be_created = [] %}
        {% set schemas_missing_in_permifrost = [] %}

        {# Get the databases for the schemas defined in permifrost #}
        {% for full_schema_name in permifrost_schemas %}
            {% set db = full_schema_name.split('.')[0] %}            
            {% if db not in permifrost_databases %}
                {{ permifrost_databases.append(db) }}
            {% endif %}
        {% endfor %}

        {# Go through each database and compare snowflake to permifrost #}
        {% for schema_db in permifrost_databases %}
            {# Get schemas for this database from snowflake #}
            {% set schemas_in_snowflake_db_query %}
                use role securityadmin;
                show schemas in database {{ schema_db }};
            {% endset %}

            {% set query_results = run_query(schemas_in_snowflake_db_query) %}

            {% set schemas_in_snowflake_db = query_results.columns["name"].values() %}

            {# We don't care about default snowflake schemas #}
            {% set excluded_schemas = ["INFORMATION_SCHEMA","PUBLIC"] %}

            {# Go through each schema that's in snowflake and see if it exists in permifrost #}
            {% for snowflake_schema in schemas_in_snowflake_db %}
                {% set full_schema_name = schema_db + '.' + snowflake_schema.upper() %}

                {% if (snowflake_schema.upper() not in excluded_schemas) and full_schema_name not in permifrost_schemas %}
                    {{ schemas_missing_in_permifrost.append(full_schema_name) }}
                {% endif %}
            {% endfor %}

            {# Go through each schema that's in permifrost and see if it exists in snowflake #}
            {% for permifrost_schema in permifrost_schemas %}

                {% set permifrost_schema_db = permifrost_schema.split('.')[0] %}
                {% set permifrost_schema_name = permifrost_schema.split('.')[1] %}

                {% if permifrost_schema_db == schema_db %}
                    {% if permifrost_schema_name not in schemas_in_snowflake_db %}
                        {{ schemas_to_be_created.append(permifrost_schema_db + "." + permifrost_schema_name) }}
                    {% endif %}
                {% endif %} 
            {% endfor %}
        {% endfor %}


        {{ print('#########################################')}}
        {{ print('####### Schemas not in Permifrost #######')}}
        {{ print('#########################################')}}

        {{ print('\n'.join(schemas_missing_in_permifrost))}}
        {{ print('\n') }}

        {% if schemas_to_be_created %}
            {{ print('########################################')}}
            {{ print('####### Schemas not in Snowflake #######')}}
            {{ print('########################################')}}

            {{ print('\n'.join(schemas_to_be_created)) }} 
            {{ print('\n') }}

            {% if dry_run == true %}
                {{ print('Schemas not created during a dry_run')}}
            {% else %}
                {{ create_snowflake_schemas(schemas_to_be_created) }}
            {% endif %}

        {% else %}

            {{ print('=========================================')}}
            {{ print('Schemas in Permifrost exist in Snowflake')}}
            {{ print('=========================================')}}

        {% endif %}
    {% endif %}

{% endmacro %}
