select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from BALBOA_STAGING.DBT_TEST__AUDIT.dbt_utils_unique_combination_o_92a4498bca967cf13736e916abf6a066
    
      
    ) dbt_internal_test