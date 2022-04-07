select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from staging_BALBOA.dbt_test__audit.dbt_expectations_expect_groupe_faf0873fa4b08ba538c7cfe018b7e925
    
      
    ) dbt_internal_test