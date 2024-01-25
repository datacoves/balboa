with raw_source as (

    select *
    from {{ source('US_POPULATION', 'US_POPULATION') }}

),

final as (

    select
        "ID"::number as id,
        "STATES"::varchar as state_name,
        replace("_2010", ',', '')::integer as "2010",
        replace("_2011", ',', '')::integer as "2011",
        replace("_2012", ',', '')::integer as "2012",
        replace("_2013", ',', '')::integer as "2013",
        replace("_2014", ',', '')::integer as "2014",
        replace("_2015", ',', '')::integer as "2015",
        replace("_2016", ',', '')::integer as "2016",
        replace("_2017", ',', '')::integer as "2017",
        replace("_2018", ',', '')::integer as "2018",
        replace("_2019", ',', '')::integer as "2019"

    from raw_source

)

select * from final
