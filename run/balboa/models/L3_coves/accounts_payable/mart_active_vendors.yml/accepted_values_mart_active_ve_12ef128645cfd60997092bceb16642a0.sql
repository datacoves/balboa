
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from BALBOA_STAGING.DBT_TEST__AUDIT.accepted_values_mart_active_ve_12ef128645cfd60997092bceb16642a0
    
      
    ) dbt_internal_test