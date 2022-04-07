select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from staging_BALBOA.dbt_test__audit.unique_dim_dbt__sources_manifest_source_id
    
      
    ) dbt_internal_test