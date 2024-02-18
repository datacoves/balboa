with
    personal_loans as (
        select
            annual_inc,
            int_rate,
            zip_code,
            substring(zip_code, 1, 3) as zip
        from {{ ref("_airbyte_raw_personal_loans") }}
        where (annual_inc < '2000000')
    ),

    final as (
        select
            personal_loans.int_rate,
            zip_coordinates.lat,
            zip_coordinates.lon
        from personal_loans
        left outer join
            {{ ref("_airbyte_raw_zip_coordinates") }} as zip_coordinates
            on personal_loans.zip = zip_coordinates.zip
    )

select * from final
