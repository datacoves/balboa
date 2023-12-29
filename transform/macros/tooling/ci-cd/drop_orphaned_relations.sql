


{# Macro for deleting objects from the data warehouse that are no longer in dbt. #}
{#
    dbt run-operation drop_orphaned_relations --args '{"dry_run": true}'
#}

{%- macro drop_orphaned_relations(dry_run='true') -%}

    {%- if execute -%}

        -- Create empty dictionary that will contain the hierarchy of the models in dbt
        {%- set current_model_locations = {} -%}

        -- Insert the hierarchy database.schema.table in the dictionary above
        {%- for node in graph.nodes.values() | selectattr("resource_type", "in", ["model", "seed", "snapshot"]) -%}

            {%- set database_name = node.database.upper() -%}
            {%- set schema_name = node.schema.upper() -%}
            {%- set table_name = node.alias if node.alias else node.name -%}

            -- Add db name if it does not exist in the dict
            {%- if not database_name in current_model_locations -%}
                {% do current_model_locations.update({database_name: {}}) -%}
            {%- endif -%}

            -- Add schema name if it does not exist in the dict
            {%- if not schema_name in current_model_locations[database_name] -%}
                {% do current_model_locations[database_name].update({schema_name: []}) -%}
            {%- endif -%}

            -- Add the tables for the db and schema selected
            {%- do current_model_locations[database_name][schema_name].append(table_name.upper()) -%}

        {%- endfor -%}

    {%- endif -%}

    -- Query to retrieve the models to drop
    {%- set cleanup_query -%}

        WITH models_to_drop AS (
            {%- for database in current_model_locations.keys() -%}
                {%- if loop.index > 1 %}
                UNION ALL
                {% endif %}

                SELECT
                    CASE
                        WHEN table_type = 'BASE TABLE' THEN 'TABLE'
                        WHEN table_type = 'VIEW' THEN 'VIEW'
                        ELSE NULL
                    END AS relation_type,
                    table_catalog,
                    table_schema,
                    table_name,
                    concat_ws('.', table_catalog, table_schema, table_name) as relation_name
                FROM {{ database }}.information_schema.tables
                WHERE
                    table_schema IN ('{{ "', '".join(current_model_locations[database].keys()) }}')
                    AND NOT (
                        {%- for schema in current_model_locations[database].keys() -%}
                            {% if loop.index > 1 %}
                            OR {% endif %} table_schema = '{{ schema }}' AND table_name IN ('{{ "', '".join(current_model_locations[database][schema]) }}')
                        {%- endfor %}
                    )
            {%- endfor -%}
        )
        -- Create the DROP statments to be executed in the database
        SELECT 'DROP ' || relation_type || ' IF EXISTS ' || table_catalog || '.' ||table_schema || '.' || table_name || ';' AS drop_commands
        FROM models_to_drop
        WHERE relation_type IS NOT NULL

    {%- endset -%}

    {{ print(cleanup_query) }}

    -- Execute the DROP statments above
    {%- set drop_commands = run_query(cleanup_query) -%}

    {%- if drop_commands -%}

        {%- for drop_command in drop_commands.columns[0].values() -%}
            {% if loop.first %}
                {{ print('Drop commands:') }}
                {{ print('-' * 20) }}
            {% endif %}

            {%- do print(drop_command) -%}

            {%- if not dry_run -%}
                {%- do run_query(drop_command) -%}
                {{ print('Drop command executed') }}
            {%- endif -%}

        {%- endfor -%}

        {{ print('-' * 20) }}

        {%- if dry_run -%}
            {{ print('Dry run; orphaned relations still in database') }}
        {%- else -%}
            {{ print('Done dropping orphaned relations from database') }}
        {%- endif -%}

    {%- else -%}

        {{ print('No orphaned relations found') }}

    {%- endif -%}

{%- endmacro -%}
