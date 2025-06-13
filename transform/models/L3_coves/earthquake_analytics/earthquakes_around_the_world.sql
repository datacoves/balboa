with country_polygons as (

    Select
        country_name,
        country_code_2,
        geography
    FROM {{ ref("stg_country_polygons") }}

),

earthquakes As (
    select
        location_geo_point,
        sig,
        case
            when sig < 100 then 'Low'
            when sig >= 100 and sig < 500 then 'Moderate'
            else 'High'
        end as sig_class,
        earthquake_date
    from {{ ref("stg_earthquakes") }}
    {% if is_incremental() %}
        where earthquake_date > (select max(earthquake_date) from {{ this }})
    {% endif %}

),

final AS (

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
