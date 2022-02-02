with raw_source as (

    select
        *
    from {{ source('raw', 'lineage_processing') }}

),

final as (

    select
        value:"c1"::integer as c1,
        value:"c2"::integer as c2,
        value:"c3"::integer as c3,
        value:"c4"::integer as c4,
        value:"c5"::varchar as c5,
        value:"c7"::varchar as c7,
        "SOURCE" as source,
        "SOURCE_OBJECT" as source_object,
        "PROCESS" as process,
        "DESTINATION" as destination,
        "DESTINATION_OBJECT" as destination_object,
        "COMMENT" as comment,
        "DATA_LINEAGE_EXISTANCE_CHECK" as data_lineage_existance_check

    from raw_source

)

select * from final

