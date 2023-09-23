select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from BALBOA_STAGING.dbt_test__audit.dbt_utils_unique_combination_of_columns_country_populations_country_code
    
      
    ) dbt_internal_test