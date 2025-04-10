
  create or replace   view BALBOA_STAGING.L1_COUNTRY_GEO.stg_country_polygons
  
    
    
(
  
    "COUNTRY_NAME" COMMENT $$The name of the country represented by the polygon.$$, 
  
    "COUNTRY_CODE_2" COMMENT $$The two-letter ISO 3166-1 alpha-2 code of the country.$$, 
  
    "COUNTRY_CODE_3" COMMENT $$The three-letter ISO 3166-1 alpha-3 code of the country.$$, 
  
    "FEATURE_TYPE" COMMENT $$The type of geographical feature the polygon represents, such as a country or territory.$$, 
  
    "GEOMETRY_TYPE" COMMENT $$The geometric shape type used for the polygon, such as Polygon or Multipolygon.$$, 
  
    "GEOGRAPHY" COMMENT $$The spatial representation of the country’s polygon in a format suitable for geographic information systems (GIS).$$, 
  
    "AREA_M2" COMMENT $$The area of the polygon in square meters, representing the geographical size of the country.$$, 
  
    "PERIMETER_M" COMMENT $$The perimeter of the polygon in meters, indicating the boundary length of the country’s representation.$$, 
  
    "CENTROID" COMMENT $$The centroid point of the polygon, typically used to determine the geographic center or balancing point of the shape.$$, 
  
    "RAW_GEOJSON" COMMENT $$The raw GeoJSON data used to create and describe the country’s polygon features, suitable for mapping applications.$$
  
)

  copy grants as (
    with raw_source as (

    select *
    from RAW.COUNTRY_GEO.COUNTRY_POLYGONS

),

final as (

    select
        features:properties:ADMIN::STRING as country_name,
        features:properties:ISO_A2::STRING as country_code_2,
        features:properties:ISO_A3::STRING as country_code_3,
        features:type::STRING as feature_type,
        features:geometry:type::STRING as geometry_type,
        TRY_TO_GEOGRAPHY(features:geometry) as geography,
        features as raw_geojson
    from raw_source

)

select
    country_name,
    country_code_2,
    country_code_3,
    feature_type,
    geometry_type,
    geography,
    case when geography is not NULL then ST_AREA(geography) end as area_m2,
    case when geography is not NULL then ST_PERIMETER(geography) end as perimeter_m,
    case when geography is not NULL then ST_CENTROID(geography) end as centroid,
    raw_geojson
from final
  );

