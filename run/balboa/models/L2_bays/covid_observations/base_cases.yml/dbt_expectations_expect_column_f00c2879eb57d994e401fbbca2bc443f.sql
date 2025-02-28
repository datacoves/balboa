select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from BALBOA_STAGING.DBT_TEST__AUDIT.dbt_expectations_expect_column_f00c2879eb57d994e401fbbca2bc443f
    
      
    ) dbt_internal_test