with raw_source as (

    select *
    from {{ source('loans', '_airbyte_raw_zip_coordinates') }}

),

final as (

    select *
    from raw_source

)

select * from final
