with final as (

    select
        *,
        difference as new_cases
    from BALBOA.l1_starschema_covid19.jhu_covid_19

)

select * from final