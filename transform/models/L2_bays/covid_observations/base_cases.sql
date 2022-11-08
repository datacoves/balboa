with final as (

    select
        *,
        difference as new_cases
    from {{ source('starschema_covid19', 'jhu_covid_19') }}

)

select * from final
