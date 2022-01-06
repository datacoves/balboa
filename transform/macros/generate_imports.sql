{# Macro to import sources for table as CTEs #}
{# Usage:
    {{ generate_imports(
        [
            'model_1'.
            'model_2',
            ...
        ]
    ) }},
    
    cte_logic as (
        any non-import CTEs that are required in this model
    )
#}

{% macro generate_imports(model_list) %}

WITH 
{% for cte_ref in model_list %} 
{{cte_ref}} AS (

SELECT * 
FROM {{ ref(cte_ref) }}
){# Add a comma after each CTE except the last one #} {%- if not loop.last -%},{%- endif -%}
{%- endfor -%}

{%- endmacro %}
