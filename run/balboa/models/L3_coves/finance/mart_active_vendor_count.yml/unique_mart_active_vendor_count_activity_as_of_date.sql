
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from BALBOA_STAGING.DBT_TEST__AUDIT.unique_mart_active_vendor_count_activity_as_of_date
    
      
    ) dbt_internal_test