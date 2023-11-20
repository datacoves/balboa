with loans as (

    select *
    from {{ ref('personal_loans') }}

),

final as (

    select
        "ADDR_STATE"::varchar as addr_state,
        count(*) as total_loans
    from loans
    group by 1
    order by 2 desc

)

select * from final
limit 10
