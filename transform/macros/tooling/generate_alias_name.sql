
{#
This is an example of how to override aliases with dbt

{% macro generate_alias_name__DISABLED(custom_alias_name=none, node=none) -%}

    {% if ("__" in node.name) and ("dbt_packages" not in node.root_path)%}
        {{ node.name.split("__")[0] }}
    {%- else -%}
        {{ node.name }}
    {%- endif -%}

{%- endmacro %} #}
