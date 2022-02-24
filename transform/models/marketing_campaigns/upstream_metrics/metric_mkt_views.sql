with
views as (
    select
        campaign_name	
        , campaign_status	
        , campaign_tag	
        , target_list_name	
        , 'views' as metric_name	
        , sum(views) as metric_value
    from {{ ref('metric_source_data') }}
    group by 
        campaign_name	
        , campaign_status	
        , campaign_tag	
        , target_list_name	
        , metric_name	    
)

select * from views