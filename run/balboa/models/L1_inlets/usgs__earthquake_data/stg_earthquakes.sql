
  create or replace   view BALBOA_STAGING.L1_USGS__EARTHQUAKE_DATA.stg_earthquakes
  
    
    
(
  
    "EARTHQUAKE_ID" COMMENT $$A unique identifier for each earthquake event.$$, 
  
    "LONGITUDE" COMMENT $$The longitude coordinate where the earthquake occurred.$$, 
  
    "LATITUDE" COMMENT $$The latitude coordinate where the earthquake occurred.$$, 
  
    "ELEVATION" COMMENT $$The elevation in meters at the location of the earthquake.$$, 
  
    "LOCATION_GEO_POINT" COMMENT $$The geographical point combining latitude and longitude, representing the exact location of the earthquake.$$, 
  
    "TITLE" COMMENT $$A brief title or description of the earthquake event.$$, 
  
    "PLACE" COMMENT $$The name or description of the nearest geographic location affected by the earthquake.$$, 
  
    "SIG" COMMENT $$The significance of the earthquake, a calculated value used to express its overall impact.$$, 
  
    "MAG" COMMENT $$The magnitude of the earthquake, representing its size or energy release.$$, 
  
    "MAGTYPE" COMMENT $$The type or classification method used to determine the earthquake magnitude.$$, 
  
    "EARTHQUAKE_DATE" COMMENT $$The date and time when the earthquake occurred.$$, 
  
    "RECORD_UPDATED_DATE" COMMENT $$The date and time when the earthquake data was last updated.$$
  
)

  copy grants as (
    with raw_source as (

    select *
    from RAW.USGS__EARTHQUAKE_DATA.EARTHQUAKES

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
  );

