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