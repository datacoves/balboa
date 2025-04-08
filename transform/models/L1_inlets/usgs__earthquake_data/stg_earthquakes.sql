with raw_source as (

    select *
    from {{ source('USGS__EARTHQUAKE_DATA', 'EARTHQUAKES') }}

),

final as (

    select
        id as earthquake_id,
        geometry:"coordinates"[0]::double as longitude,
        geometry:"coordinates"[1]::double as latitude,
        geometry:"coordinates"[2]::double as elevation,
        st_point(longitude, latitude) as location_geo_point,
        properties:"title"::varchar as title,
        properties:"place"::varchar as place,
        properties:"sig"::varchar as sig,
        properties:"mag"::varchar as mag,
        properties:"magType"::varchar as magtype,
        to_date(properties:"time"::varchar) as earthquake_date,
        to_date(properties:"updated"::varchar) as record_updated_date
    from raw_source

)

select * from final
