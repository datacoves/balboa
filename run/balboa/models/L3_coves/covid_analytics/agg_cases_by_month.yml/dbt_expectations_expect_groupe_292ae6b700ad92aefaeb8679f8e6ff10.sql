select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from BALBOA_STAGING.dbt_test__audit.dbt_expectations_expect_grouped_row_values_to_have_recent_data_agg_cases_by_month_month
    
      
    ) dbt_internal_test