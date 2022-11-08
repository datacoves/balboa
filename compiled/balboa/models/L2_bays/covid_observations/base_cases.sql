with final as (

    select
        *,
        difference as new_cases
    from starschema_covid19.public.JHU_COVID_19

)

select * from final