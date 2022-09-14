select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from BALBOA_STAGING.dbt_test__audit.check_critical_rows_exist_in_seed_covid_cases_ref_covid_cases_expected_values_
    
      
    ) dbt_internal_test