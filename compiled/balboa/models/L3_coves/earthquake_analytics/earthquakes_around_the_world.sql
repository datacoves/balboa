

with country_polygons as (

    select
        country_name,
        country_code_2,
        geography
    from BALBOA.L1_COUNTRY_GEO.stg_country_polygons

),

earthquakes as (
    select
        location_geo_point,
        sig,
        case
            when sig < 100 then 'Low'
            when sig >= 100 and sig < 500 then 'Moderate'
            else 'High'
        end as sig_class,
        earthquake_date
    from BALBOA.L1_USGS__EARTHQUAKE_DATA.stg_earthquakes
    
        where earthquake_date > (select max(earthquake_date) from BALBOA.L3_EARTHQUAKE_ANALYTICS.earthquakes_around_the_world)
    

),

final as (

    select
        earthquakes.*,
        country_polygons.country_code_2 as country_code
    from earthquakes, country_polygons
    where country_polygons.geography is not NULL
        and st_contains(
            country_polygons.geography,
            earthquakes.location_geo_point
        )
)

select * from final