with

source as ( select * from {{ ref('metric_source_data') }} ),

revenue as (
    select
        campaign_name	
        , campaign_status	
        , campaign_tag	
        , target_list_name	
        , 'revenue' as metric_name	
        , sum(revenue) as metric_value
    from source
    group by 
        campaign_name	
        , campaign_status	
        , campaign_tag	
        , target_list_name	
        , metric_name	    
),

week_to_date as (
    select
        campaign_name	
        , campaign_status	
        , campaign_tag	
        , target_list_name	
        , 'revenue_WTD' as metric_name	
        , sum(revenue) as metric_value
    from source
    where date_trunc(week, campaign_execution_datetime :: date) = date_trunc(week, current_date())
    group by 
        campaign_name	
        , campaign_status	
        , campaign_tag	
        , target_list_name	
        , metric_name	    
),

month_to_date as (
    select
        campaign_name	
        , campaign_status	
        , campaign_tag	
        , target_list_name	
        , 'revenue_MTD' as metric_name	
        , sum(revenue) as metric_value
    from source
    where date_trunc(month, campaign_execution_datetime :: date) = date_trunc(month, current_date())
    group by 
        campaign_name	
        , campaign_status	
        , campaign_tag	
        , target_list_name	
        , metric_name	    
),

quarter_to_date as (
    select
        campaign_name	
        , campaign_status	
        , campaign_tag	
        , target_list_name	
        , 'revenue_QTD' as metric_name	
        , sum(revenue) as metric_value
    from source
    where date_trunc(quarter, campaign_execution_datetime :: date) = date_trunc(quarter, current_date())
    group by 
        campaign_name	
        , campaign_status	
        , campaign_tag	
        , target_list_name	
        , metric_name	    

),

year_to_date as (
    select
        campaign_name	
        , campaign_status	
        , campaign_tag	
        , target_list_name	
        , 'revenue_YTD' as metric_name	
        , sum(revenue) as metric_value
    from source
    where date_trunc(year, campaign_execution_datetime :: date) = date_trunc(year, current_date())
    group by 
        campaign_name	
        , campaign_status	
        , campaign_tag	
        , target_list_name	
        , metric_name	    

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

