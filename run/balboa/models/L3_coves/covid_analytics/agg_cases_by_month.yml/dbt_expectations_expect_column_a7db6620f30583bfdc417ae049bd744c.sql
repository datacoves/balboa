select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from BALBOA_STAGING.dbt_test__audit.dbt_expectations_expect_column_distinct_values_to_contain_set_agg_cases_by_month_state
    
      
    ) dbt_internal_test