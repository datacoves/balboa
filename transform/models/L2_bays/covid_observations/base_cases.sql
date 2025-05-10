with final as (

    select
        *,
        difference as new_cases
    from {{ ref('stg_jhu_covid_19') }}
)

select * from final
