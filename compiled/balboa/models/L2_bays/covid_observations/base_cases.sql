with final as (

    select
        *,
        difference as new_cases
    from BALBOA.L1_COVID19_EPIDEMIOLOGICAL_DATA.stg_jhu_covid_19
)

select * from final