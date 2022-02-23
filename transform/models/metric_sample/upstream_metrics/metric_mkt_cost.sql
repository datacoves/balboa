with
renamed as (
    select
        campaign_name	
        , campaign_execution_datetime	
        , campaign_status	
        , campaign_tag	
        , target_list_name	
        , metric_name	
        , metric_value
    from {{ ref('metric_2_cost') }}
)

select * from renamed


