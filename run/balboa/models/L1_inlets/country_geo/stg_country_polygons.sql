
  create or replace   view BALBOA_STAGING.L1_COUNTRY_GEO.stg_country_polygons
  
    
    
(
  
    "COUNTRY_NAME" COMMENT $$The name of the country represented by the polygon.$$, 
  
    "COUNTRY_CODE_2" COMMENT $$The two-letter ISO 3166-1 alpha-2 code of the country.$$, 
  
    "COUNTRY_CODE_3" COMMENT $$The three-letter ISO 3166-1 alpha-3 code of the country.$$, 
  
    "GEOMETRY_TYPE" COMMENT $$The geometric shape type used for the polygon, such as Polygon or Multipolygon.$$, 
  
    "GEOGRAPHY" COMMENT $$The spatial representation of the country’s polygon in a format suitable for geographic information systems (GIS).$$, 
  
    "AREA_M2" COMMENT $$The area of the polygon in square meters, representing the geographical size of the country.$$, 
  
    "PERIMETER_M" COMMENT $$The perimeter of the polygon in meters, indicating the boundary length of the country’s representation.$$, 
  
    "CENTROID" COMMENT $$The centroid point of the polygon, typically used to determine the geographic center or balancing point of the shape.$$
  
)

  copy grants
  
  
  as (
    with raw_source as (

    select *
    from RAW.COUNTRY_GEO.COUNTRY_POLYGONS

),

final as (

    select
        name as country_name,
        iso3166_1_alpha_2 as country_code_2,
        iso3166_1_alpha_3 as country_code_3,
        geometry_type,
        try_to_geography(geometry) as geography
    from raw_source

)


select
    country_name,
    country_code_2,
    country_code_3,
    geometry_type,
    geography,
    case when geography is not null then st_area(geography) end as area_m2,
    case when geography is not null then st_perimeter(geography) end as perimeter_m,
    case when geography is not null then st_centroid(geography) end as centroid
from final
  );

