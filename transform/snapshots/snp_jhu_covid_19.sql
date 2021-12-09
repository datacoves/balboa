{% snapshot snp_jhu_covid_19 %}

{{
    config(
      unique_key="country_region||'|'||province_state||'|'||county||'|'||date||'|'||case_type",
      target_schema=target.schema,
      strategy='timestamp',
      updated_at='last_updated_date',

    )
}}

select * from {{ source('starschema_covid19', 'jhu_covid_19') }}

{% endsnapshot %}