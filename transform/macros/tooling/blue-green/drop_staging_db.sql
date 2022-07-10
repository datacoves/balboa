{# This macro drops test databases #}
{#
    To run: 
    dbt run-operation drop_staging_db --args 'db_name: demo_db' 
#}

{%- macro drop_staging_db(db_name) -%}

    {# Only want this for stating with staging #}
    {% if not db_name.startswith('staging')%}
        {{ log("Database is not a staging db: " ~ db_name, true)}}
        {{ exceptions.raise_compiler_error("Not a staging Database") }}
    {% else %}
        {{ drop_recreate_db(db_name = db_name, recreate = False) }}
        {{ log("Deteled " + db_name, true) }}
    {% endif %} #}

{%- endmacro -%}