{% macro drop_old_relations(
        dry_run = 'false'
    ) %}
    {% if execute %}
        {% set current_models = [] %}
        {% for node in graph.nodes.values() | selectattr(
                "resource_type",
                "in",
                ["model", "seed", "snapshot"]
            ) %}
            {% if node.alias %}
                {% do current_models.append(
                    (node.alias, node.schema)
                ) %}
            {% elif node.config.materialization != "ephemeral" %}
                {% do current_models.append(
                    (node.name, node.schema)
                ) %}
            {% endif %}
        {% endfor %}
    {% endif %}

    {% set cleanup_query %}
    WITH models_to_drop AS (
        SELECT
            CASE
                WHEN table_type = 'BASE TABLE' THEN 'TABLE'
                WHEN table_type = 'VIEW' THEN 'VIEW'
            END AS relation_type,
            concat_ws(
                '.',
                table_catalog,
                table_schema,
                '"' || table_name || '"'
            ) AS relation_name
        FROM
            {{ target.database }}.information_schema.tables
        WHERE
            table_schema ILIKE '{{ target.schema }}%'
            AND relation_name NOT IN ({%- for model, schema in current_models -%}
                '{{ target.database }}.{{ schema.upper() }}."{{ model.upper() }}"' {%- if not loop.last -%},
                {% endif %}
            {%- endfor -%})
    )
SELECT
    'DROP ' || relation_type || ' ' || relation_name || ';' AS drop_commands
FROM
    models_to_drop -- intentionally exclude unhandled table_types, including 'external table`
WHERE
    drop_commands IS NOT NULL {% endset %}
    {% set drop_commands = run_query(cleanup_query).columns [0].values() %}
    {% if drop_commands %}
        {% for drop_command in drop_commands %}
            {% do log(
                drop_command,
                True
            ) %}
            {% if dry_run == 'false' %}
                {% do run_query(drop_command) %}
            {% endif %}
        {% endfor %}
    {% else %}
        {% do log(
            'No relations to clean.',
            True
        ) %}
    {% endif %}
{%- endmacro -%}
