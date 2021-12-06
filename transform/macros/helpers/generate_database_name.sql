{% macro generate_database_name(custom_database_name=none, node=none) -%}
    {# If custom database is set (raw), use that. If target is prd, use default; otherwise append target name to default #}

    {%- set default_database = target.database -%}
    {%- if custom_database_name is none -%}
        {%- if target.name == 'prd' -%}
            {{ default_database }}
        {%- else -%}
            {{ default_database }}_{{ target.name | upper }}
        {%- endif -%}

    {%- else -%}

        {{ custom_database_name | trim }}

    {%- endif -%}

{%- endmacro %}