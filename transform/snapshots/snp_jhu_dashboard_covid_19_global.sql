{% snapshot snp_jhu_dashboard_covid_19_global %}

{{
    config(
      unique_key="ifnull(country_region,'') || '|' || ifnull(province_state,'') || '|' || ifnull(county,'') || '|' || to_varchar(date)",
      target_schema=target.schema,
      strategy='timestamp',
      updated_at='last_update_date',

    )
}}

select * from {{ source('starschema_covid19', 'jhu_dashboard_covid_19_global') }}

{% endsnapshot %}