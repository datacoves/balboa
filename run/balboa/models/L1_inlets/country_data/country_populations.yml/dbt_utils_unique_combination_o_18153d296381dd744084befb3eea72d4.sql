select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from BALBOA_STAGING.DBT_TEST__AUDIT.dbt_utils_unique_combination_o_18153d296381dd744084befb3eea72d4
    
      
    ) dbt_internal_test