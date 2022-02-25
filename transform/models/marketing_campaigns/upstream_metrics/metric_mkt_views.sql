{% set dimensions_list = metric_prep() %}

select

    {% for dim in dimensions_list %}
    {{dim}},
    {% endfor %}

    'VIEWS' as metric_name,
    sum(views) as metric_value

from {{ ref('metric_source_data') }}

group by 
    {% for dim in dimensions_list %}
    {{dim}},
    {% endfor %}
    metric_name
