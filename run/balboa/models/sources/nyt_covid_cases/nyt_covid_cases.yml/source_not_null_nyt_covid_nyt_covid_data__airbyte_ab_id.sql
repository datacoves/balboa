select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from BALBOA_STAGING.dbt_test__audit.source_not_null_nyt_covid_nyt_covid_data__airbyte_ab_id
    
      
    ) dbt_internal_test