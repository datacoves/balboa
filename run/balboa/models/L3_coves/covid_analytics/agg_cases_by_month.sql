
  
    

        create or replace transient table BALBOA_STAGING.l3_covid_analytics.agg_cases_by_month copy grants as
        (select *
from -- depends on: BALBOA_STAGING.dbt_metrics.dbt_metrics_default_calendar
    

(

with calendar as (

    select 
        * 
    from BALBOA_STAGING.dbt_metrics.dbt_metrics_default_calendar
    where date_day >= cast('2020-01-01' as date) 
)



, sum_cases__aggregate as (
    select
        date_month,
        country,
        state,
        sum(property_to_aggregate) as sum_cases,
        boolor_agg(metric_date_day is not null) as has_data
        

    from (

    
        select 
        
            cast(base_model.date as date) as metric_date_day, -- timestamp field
            calendar_table.date_month as date_month,
            calendar_table.date_day as window_filter_date,

            
                base_model.country,
                base_model.state,
                (cases) as property_to_aggregate

        from BALBOA_STAGING.l3_covid_analytics.covid_cases_state base_model 
        left join BALBOA_STAGING.dbt_metrics.dbt_metrics_default_calendar calendar_table

        
            on cast(base_model.date as date) = calendar_table.date_day
        

        where 1=1
            and (
            cast(date as date) >= cast('2020-01-01' as date) 
            )
        
    ) as base_query

    where 1=1

    

    
        group by
        1, 2, 3



)

, sum_cases__dims as (
    select distinct
        
        country,
        state
        
    from sum_cases__aggregate
)

, sum_cases__spine_time as (

    select
        calendar.date_month
        , sum_cases__dims.country
        , sum_cases__dims.state

    from calendar
    cross join sum_cases__dims
    
        group by
        1, 2, 3



)

, sum_cases__final as (
    
    select
        
        parent_metric_cte.date_month,
        parent_metric_cte.country,
        parent_metric_cte.state,
        coalesce(sum_cases, 0) as sum_cases

    from sum_cases__spine_time as parent_metric_cte
    left outer join sum_cases__aggregate
        using (
            date_month
            , country
            , state
        )

        where (
            parent_metric_cte.date_month <= (
                select 
                    max(case when has_data then date_month end) 
                from sum_cases__aggregate
            ) 
        )      
         
    )



, secondary_calculations as (

    select 
        *


    from sum_cases__final
)





    select * from sum_cases__final
        order by
        1 desc, 2 desc, 3 desc



) metric_subq
order by 1 desc



        );
      
  