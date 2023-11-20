{% snapshot snp_jhu_dashboard_covid_19_global %}
{{
    config(
        unique_key="ifnull(country_region,'') || '|' || ifnull(province_state,'') || '|' || ifnull(county,'') || '|' || to_varchar(date) || last_update_date",
        strategy='timestamp',
        updated_at='last_update_date'
    )
}}

    select distinct
        country_region,
        province_state,
        county,
        fips,
        date,
        active,
        people_tested,
        confirmed,
        people_hospitalized,
        deaths,
        recovered,
        incident_rate,
        testing_rate,
        hospitalization_rate,
        long,
        lat,
        iso3166_1,
        iso3166_2,
        last_update_date
    from
        {{ source('covid19_epidemiological_data', 'jhu_dashboard_covid_19_global') }}
{% endsnapshot %}
