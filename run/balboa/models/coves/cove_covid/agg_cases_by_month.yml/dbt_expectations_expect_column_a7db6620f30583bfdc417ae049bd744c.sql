select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from BALBOA_STAGING.dbt_test__audit.dbt_expectations_expect_column_a7db6620f30583bfdc417ae049bd744c
    
      
    ) dbt_internal_test