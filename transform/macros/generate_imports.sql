{# Macro to import sources for table as CTEs #}
{# Usage:
    {{ generate_imports(
        [
            ( 'alias_model_1', ref('model_1') ),
            ( 'alias_model_2', ref('model_2') ),
            ...
        ]
    ) }}
    , cte_logic as (
        any non-import CTEs that are required in this model
    )
#}

{% macro generate_imports(tuple_list) %}

WITH{% for cte_ref in tuple_list %} {{cte_ref[0]}} AS (

    SELECT * 
    FROM {{ cte_ref[1] }}

)
    {%- if not loop.last -%}
    ,
    {%- endif -%}
    
    {%- endfor -%}

{%- endmacro %}
