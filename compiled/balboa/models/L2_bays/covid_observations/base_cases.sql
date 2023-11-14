with final as (

    select
        *,
        difference as new_cases
    from BALBOA.l1_covid19_epidemiological_data.jhu_covid_19

)

select * from final