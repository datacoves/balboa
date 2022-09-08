select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from BALBOA_STAGING.dbt_test__audit.check_critical_rows_exist_in_s_cbc1c03738dd375dd81393e0971baa0c
    
      
    ) dbt_internal_test