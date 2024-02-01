select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from balboa_STAGING.dbt_test__audit.not_null_personal_loans_addr_state
    
      
    ) dbt_internal_test