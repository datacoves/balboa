{% snapshot snp_jhu_dashboard_covid_19_global %}
    {{
        config(
            unique_key="ifnull(country_region,'') || '|' || ifnull(province_state,'') || '|' || ifnull(county,'') || '|' || to_varchar(date)",
            strategy='timestamp',
            updated_at='last_update_date'
        )
    }}

    sa
        {{ source('starschema_covid19', 'jhu_dashboard_covid_19_global') }}

{% endsnapshot %}
