with raw_source as (

    select *
    from RAW.RAW.COUNTRY_POPULATIONS

),

final as (

    select
        year,
        year - 1 as prior_year,
        "COUNTRY NAME" as country_name,
        value,
        "COUNTRY CODE" as country_code

    from raw_source

)

select * from final
order by country_code