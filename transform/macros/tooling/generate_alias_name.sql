{% macro generate_alias_name(custom_alias_name=none, node=none) -%}

    {% if ("__" in node.name) and ("dbt_packages" not in node.root_path)%}
        {{ node.name.split("__")[0] }}
    {%- else -%}
        {{ node.name }}
    {%- endif -%} 

{%- endmacro %}
