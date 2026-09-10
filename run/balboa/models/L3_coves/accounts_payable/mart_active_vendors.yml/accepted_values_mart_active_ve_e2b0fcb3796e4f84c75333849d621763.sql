
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from BALBOA_STAGING.DBT_TEST__AUDIT.accepted_values_mart_active_ve_e2b0fcb3796e4f84c75333849d621763
    
      
    ) dbt_internal_test