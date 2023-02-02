{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = node.config.target_schema or target.schema -%}
    
    {%- if custom_schema_name is none or target.name == 'dev' -%}

        {{ default_schema | trim }}

    {%- else -%}
    
        {{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}
