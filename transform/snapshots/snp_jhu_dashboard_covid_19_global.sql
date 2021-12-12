{% snapshot snp_jhu_dashboard_covid_19_global %}

{{
    config(
      unique_key="country_region||'|'||province_state||'|'||county||'|'||date",
      target_schema=target.schema,
      strategy='timestamp',
      updated_at='last_update_date',

    )
}}

select * from {{ source('starschema_covid19', 'jhu_dashboard_covid_19_global') }}

{% endsnapshot %}