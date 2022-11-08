

with all_values as (

    select distinct
        state as value_field

    from BALBOA.l3_covid_analytics.agg_cases_by_month
    

),
set_values as (

    select
        'Arizona' as value_field
    union all
    select
        'California' as value_field
    
    

),
unique_set_values as (

    select distinct value_field
    from
        set_values

),
validation_errors as (
    -- values in set that are not in the list of values from the model
    select
        s.value_field
    from
        unique_set_values s
        left join
        all_values v on s.value_field = v.value_field
    where
        v.value_field is null

)

select *
from validation_errors

