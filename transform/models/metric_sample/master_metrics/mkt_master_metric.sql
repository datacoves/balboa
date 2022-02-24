{% set  dimension_list = [
    'CAMPAIGN_NAME'
    , 'CAMPAIGN_EXECUTION_DATETIME'
    , 'CAMPAIGN_STATUS'
    , 'CAMPAIGN_TAG'
    , 'TARGET_LIST_NAME'
    , 'METRIC_NAME'
    , 'METRIC_VALUE'
] %}

{% set metric_models = dbt_utils.get_relations_by_pattern(
    schema_pattern='joe',
    table_pattern='metric_mkt_%',
    database='balboa_dev'
) %}

with 
unioned_data as (
    {{ dbt_utils.union_relations(
        relations=metric_models,
        include=dimension_list,
        column_override= {"CAMPAIGN_EXECUTION_DATETIME":"DATE"}
    ) }}
),

-- this part is a little sloppy. it includes some additional requirements that negate some flexibility
-- written like this, campaign_execution_datetime is a required field in the dimensions_list
-- additionally, metric_value has to be the final field in the dimension_list or else the for loop won't work right
-- the most robust solution would be to take the two dbt_utils macros used above and rework them exactly for our purposes into one model
final as (
    select 
        {% for dim in dimension_list %}
        {% if not loop.last%} {{dim}}, {% endif %}
        {% endfor %}
        current_date() as reporting_as_of,
        coalesce (sum (case when campaign_execution_datetime :: date = current_date() then metric_value end), 0) as metric_current_date, 
        coalesce (sum (case when date_trunc(week, campaign_execution_datetime) = date_trunc(week, current_date()) then metric_value end), 0) as metric_week_to_date,
        coalesce (sum (case when date_trunc(month, campaign_execution_datetime) = date_trunc(month, current_date()) then metric_value end), 0) as metric_month_to_date,
        coalesce (sum (case when date_trunc(quarter, campaign_execution_datetime) = date_trunc(quarter, current_date()) then metric_value end), 0) as metric_quarter_to_date,
        coalesce (sum (case when date_trunc(year, campaign_execution_datetime) = date_trunc(year, current_date()) then metric_value end), 0) as metric_year_to_date
    from unioned_data
    group by 
        {% for dim in dimension_list %}
        {{dim}} {% if not loop.last%} , {% endif %}
        {% endfor %}
)

select * from final
