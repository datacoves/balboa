

{# Macro for dropping whole schemas that dbt used to build and no longer does. #}
{#
    drop_orphaned_relations only scans schemas that still contain at least one
    dbt node, so it cannot see a schema whose models were all deleted. This
    macro covers that gap. The candidate list comes from
    automate/dbt/orphaned_schemas.py, which derives it by diffing the
    production manifest against the current one:

    dbt run-operation drop_orphaned_schemas --args "{\"dry_run\": false, \"schemas\": $(../automate/dbt/orphaned_schemas.py --format json)}"

    Appearing in the production manifest is what proves dbt owned a schema, so
    schemas owned by Airbyte, dlt, Cartage, Fivetran or Snowcap are never
    candidates. This macro adds the second guard: a candidate is dropped only
    when every object still in it is one dbt built. Anything else in there --
    a table another tool created, a Snowcap stream, a stage, a task -- leaves
    the schema in place.
#}

{%- macro drop_orphaned_schemas(schemas={}, database=none, dry_run=true) -%}

    {%- if not execute -%}
        {%- do return(none) -%}
    {%- endif -%}

    {%- set target_database = (database if database else target.database) | upper -%}

    {%- if not schemas -%}
        {{ print('No orphaned schemas passed; nothing to do') }}
        {%- do return(none) -%}
    {%- endif -%}

    {{ print('Checking ' ~ schemas | length ~ ' orphaned schema(s) in ' ~ target_database) }}
    {{ print('-' * 20) }}

    {%- set dropped = [] -%}
    {%- set skipped = [] -%}

    {%- for schema_name, dbt_relations in schemas.items() -%}

        {%- set schema = schema_name | upper -%}
        {%- set known_relations = dbt_relations | map('upper') | list -%}

        {# What is actually in the schema right now, whoever created it. #}
        {%- set inventory_query -%}
            SELECT table_name, table_type, is_dynamic
            FROM {{ target_database }}.information_schema.tables
            WHERE table_schema = '{{ schema }}'
        {%- endset -%}

        {%- set schema_exists_query -%}
            SELECT count(*) AS schema_count
            FROM {{ target_database }}.information_schema.schemata
            WHERE schema_name = '{{ schema }}'
        {%- endset -%}

        {%- set schema_exists = run_query(schema_exists_query).columns[0].values()[0] > 0 -%}

        {%- if not schema_exists -%}

            {{ print(schema ~ ': not present in ' ~ target_database ~ ', nothing to drop') }}

        {%- else -%}

            {%- set inventory = run_query(inventory_query) -%}
            {%- set foreign_objects = [] -%}
            {%- set drop_commands = [] -%}

            {%- for row in inventory.rows -%}

                {%- set relation_name = row[0] | upper -%}
                {%- set table_type = row[1] | upper -%}
                {%- set is_dynamic = (row[2] | upper) == 'YES' -%}

                {%- if relation_name not in known_relations -%}

                    {# Something dbt never built moved into a dbt schema. #}
                    {%- do foreign_objects.append(relation_name ~ ' (' ~ table_type ~ ')') -%}

                {%- else -%}

                    {%- if is_dynamic -%}
                        {%- set relation_type = 'DYNAMIC TABLE' -%}
                    {%- elif table_type == 'BASE TABLE' -%}
                        {%- set relation_type = 'TABLE' -%}
                    {%- elif table_type in ['VIEW', 'MATERIALIZED VIEW', 'EXTERNAL TABLE'] -%}
                        {%- set relation_type = table_type -%}
                    {%- else -%}
                        {# An object type this macro does not know how to drop
                           safely; treat it as foreign rather than guessing. #}
                        {%- set relation_type = none -%}
                        {%- do foreign_objects.append(relation_name ~ ' (' ~ table_type ~ ')') -%}
                    {%- endif -%}

                    {%- if relation_type -%}
                        {%- do drop_commands.append(
                            'DROP ' ~ relation_type ~ ' IF EXISTS ' ~ target_database ~ '.' ~ schema ~ '.' ~ relation_name
                        ) -%}
                    {%- endif -%}

                {%- endif -%}

            {%- endfor -%}

            {%- if foreign_objects -%}

                {{ print(schema ~ ': SKIPPED, holds ' ~ foreign_objects | length ~ ' object(s) dbt did not build:') }}
                {%- for foreign_object in foreign_objects -%}
                    {{ print('    ' ~ foreign_object) }}
                {%- endfor -%}
                {{ print('    Another tool or a person owns part of this schema; drop it by hand if that is wrong.') }}
                {%- do skipped.append(schema) -%}

            {%- else -%}

                {%- for drop_command in drop_commands -%}
                    {{ print('    ' ~ drop_command) }}
                    {%- if not dry_run -%}
                        {%- do run_query(drop_command) -%}
                    {%- endif -%}
                {%- endfor -%}

                {# RESTRICT is the backstop: Snowflake refuses to drop a schema
                   that still holds anything, which covers the object types
                   information_schema.tables does not list, such as streams,
                   stages, tasks, sequences and procedures. #}
                {%- set drop_schema_command = 'DROP SCHEMA IF EXISTS ' ~ target_database ~ '.' ~ schema ~ ' RESTRICT' -%}
                {{ print('    ' ~ drop_schema_command) }}

                {%- if dry_run -%}

                    {{ print(schema ~ ': dry run, still in database') }}

                {%- else -%}

                    {%- do run_query(drop_schema_command) -%}

                    {%- set still_there = run_query(schema_exists_query).columns[0].values()[0] > 0 -%}
                    {%- if still_there -%}
                        {{ print(schema ~ ': relations dropped, but the schema still holds a non-table object and was kept') }}
                        {%- do skipped.append(schema) -%}
                    {%- else -%}
                        {{ print(schema ~ ': dropped') }}
                        {%- do dropped.append(schema) -%}
                    {%- endif -%}

                {%- endif -%}

            {%- endif -%}

        {%- endif -%}

    {%- endfor -%}

    {{ print('-' * 20) }}

    {%- if dry_run -%}
        {{ print('Dry run; no schemas were dropped') }}
    {%- else -%}
        {{ print('Dropped ' ~ dropped | length ~ ' schema(s)' ~ (': ' ~ dropped | join(', ') if dropped else '')) }}
        {%- if skipped -%}
            {{ print('Kept ' ~ skipped | length ~ ' schema(s) holding objects dbt did not build: ' ~ skipped | join(', ')) }}
        {%- endif -%}
    {%- endif -%}

{%- endmacro -%}
