with
clicks as (
    select
        campaign_name	
        , campaign_status	
        , campaign_tag	
        , target_list_name	
        , 'clicks' as metric_name	
        , sum(clicks) as metric_value
    from {{ ref('metric_source_data') }}
    group by 
        campaign_name	
        , campaign_status	
        , campaign_tag	
        , target_list_name	
        , metric_name	
)

select * from clicks


