{% macro metric_union_macro(model_reference, metric_name, dimension_list, reporting_date) %}
  {{model_reference}}_cte as
    select 
      {% for dim in dimension_list %}
      {{dim}},
      {% endfor %}
      '{{metric_name}}' as metric_name,
      current_date() as reporting_as_of,
      coalesce (sum (case when {{reporting_date}} :: date = current_date() then {{metric_name}} end), 0) as metric_current_date, 
      coalesce (sum (case when date_trunc(week, {{reporting_date}}) = date_trunc(week, current_date()) then {{metric_name}} end), 0) as metric_week_to_date,
      coalesce (sum (case when date_trunc(month, {{reporting_date}}) = date_trunc(month, current_date()) then {{metric_name}} end), 0) as metric_month_to_date,
      coalesce (sum (case when date_trunc(quarter, {{reporting_date}}) = date_trunc(quarter, current_date()) then {{metric_name}} end), 0) as metric_quarter_to_date,
      coalesce (sum (case when date_trunc(year, {{reporting_date}}) = date_trunc(year, current_date()) then {{metric_name}} end), 0) as metric_year_to_date
    from {{ref(model_reference)}}
    group by 
      {% for dim in dimension_list %} 
      {{dim}}  {% if not loop.last %},{% endif %}
      {% endfor %}

{% endmacro %}




