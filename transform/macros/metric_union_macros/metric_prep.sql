
-- query to reference a seed file that maintains valid column names for all final metric models
{% macro metric_prep() %}
  

{% set dimension_values_query  %}
select field from master_metric_fields where metric_domain = 'MARKETING_CAMPAIGN'
{% endset %}


{% if execute %}
{% set results = run_query(dimension_values_query) %}
{% set dimensions_list = results.columns[0].values() %}
-- returns a tuple of fields to loop through
{{ return(dimensions_list) }}
{% endif %}



{% endmacro %}