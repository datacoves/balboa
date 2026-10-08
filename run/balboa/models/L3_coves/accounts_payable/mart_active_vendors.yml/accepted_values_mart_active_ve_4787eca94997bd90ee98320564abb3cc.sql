
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from BALBOA_STAGING.DBT_TEST__AUDIT.accepted_values_mart_active_ve_4787eca94997bd90ee98320564abb3cc
    
      
    ) dbt_internal_test