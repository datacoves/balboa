select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from BALBOA_STAGING.dbt_test__audit.dbt_expectations_expect_groupe_9c657167f0fabb2a3fe545bb98c9b55d
    
      
    ) dbt_internal_test