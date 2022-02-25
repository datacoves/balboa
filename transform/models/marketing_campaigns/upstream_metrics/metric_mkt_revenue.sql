with

source as ( select * from {{ ref('metric_source_data') }} ),

{% set dimensions_list = metric_prep() %}


revenue as (
    select
        {% for dim in dimensions_list %}
        {{dim}},
        {% endfor %}
        'REVENUE' as metric_name,
        sum(revenue) as metric_value
    from source
    group by 
        {% for dim in dimensions_list %}
        {{dim}},
        {% endfor %}
        metric_name   
),

week_to_date as (
    select
        {% for dim in dimensions_list %}
        {{dim}},
        {% endfor %}
        'REVENUE_WTD' as metric_name,
        sum(revenue) as metric_value
    from source
    where date_trunc(week, campaign_execution_datetime :: date) = date_trunc(week, current_date())
    group by 
        {% for dim in dimensions_list %}
        {{dim}},
        {% endfor %}
        metric_name    
),

month_to_date as (
    select
        {% for dim in dimensions_list %}
        {{dim}},
        {% endfor %}
        'REVENUE_MTD' as metric_name,
        sum(revenue) as metric_value
    from source
    where date_trunc(month, campaign_execution_datetime :: date) = date_trunc(month, current_date())
    group by 
        {% for dim in dimensions_list %}
        {{dim}},
        {% endfor %}
        metric_name      
),

quarter_to_date as (
    select
        {% for dim in dimensions_list %}
        {{dim}},
        {% endfor %}
        'REVENUE_QTD' as metric_name,
        sum(revenue) as metric_value
    from source
    where date_trunc(quarter, campaign_execution_datetime :: date) = date_trunc(quarter, current_date())
    group by 
        {% for dim in dimensions_list %}
        {{dim}},
        {% endfor %}
        metric_name    
),

year_to_date as (
    select
        {% for dim in dimensions_list %}
        {{dim}},
        {% endfor %}
        'REVENUE_YTD' as metric_name,
        sum(revenue) as metric_value
    from source
    where date_trunc(year, campaign_execution_datetime :: date) = date_trunc(year, current_date())
    group by 
        {% for dim in dimensions_list %}
        {{dim}},
        {% endfor %}
        metric_name    
),

final as (
    select * from revenue
    union all
    select * from week_to_date
    union all
    select * from month_to_date
    union all
    select * from quarter_to_date
    union all
    select * from year_to_date
)

select * from final

