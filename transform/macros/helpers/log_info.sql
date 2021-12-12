{% macro log_info(message) %}
    {%- do log(dbt_utils.pretty_log_format(message), true) -%}
{% endmacro %}