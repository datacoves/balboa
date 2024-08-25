






    with grouped_expression as (
    select
        
        
    
  
( 1=1 and new_cases >= -10000000
)
 as expression


    from BALBOA.L2_COVID_OBSERVATIONS.base_cases
    

),
validation_errors as (

    select
        *
    from
        grouped_expression
    where
        not(expression = true)

)

select *
from validation_errors







