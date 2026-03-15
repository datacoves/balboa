with raw_source as (

    select *
    from RAW.RAW.COUNTRY_POPULATIONS

),

final as (

    select
        year,
        year - 1 as last_year,
        "COUNTRY_NAME" as country_name,
        value,
        "COUNTRY_CODE" as country_code

    from raw_source

)

select * from final
order by country_code