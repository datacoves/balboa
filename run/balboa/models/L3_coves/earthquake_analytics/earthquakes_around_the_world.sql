
  
    

create or replace transient table BALBOA_STAGING.L3_EARTHQUAKE_ANALYTICS.earthquakes_around_the_world
    

    copy grants
    as (

with country_polygons as (

    select
        country_name,
        country_code_2,
        geography
    from L1_COUNTRY_GEO.stg_country_polygons

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
    from L1_USGS__EARTHQUAKE_DATA.stg_earthquakes
    

),

final as (

    select
        1 as ct,
        earthquakes.*,
        country_polygons.country_code_2 as country_code
    from earthquakes, country_polygons
    where country_polygons.geography is not null
        and st_contains(
            country_polygons.geography,
            earthquakes.location_geo_point
        )
)


select * from final
    )
;


  