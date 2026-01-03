with raw_source as (

    select *
    from {{ source('COUNTRY_GEO', 'COUNTRY_POLYGONS') }}

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
