select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from BALBOA_STAGING.DBT_TEST__AUDIT.dbt_utils_unique_combination_o_68c9940147c89a986b30bf362cf57902
    
      
    ) dbt_internal_test