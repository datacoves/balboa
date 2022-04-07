select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from staging_BALBOA.dbt_test__audit.unique_dim_dbt__exposures_manifest_exposure_id_output_feeds
    
      
    ) dbt_internal_test