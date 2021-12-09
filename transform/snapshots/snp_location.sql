{% snapshot snp_location %}

{{
    config(
      unique_key='location_id',

      strategy='check',
      check_cols=['lat', 'long'],
    )
}}

select * from {{ ref('location') }}

{% endsnapshot %}