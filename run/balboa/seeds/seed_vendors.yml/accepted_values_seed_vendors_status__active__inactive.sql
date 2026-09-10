
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from BALBOA.DBT_TEST__AUDIT.accepted_values_seed_vendors_status__active__inactive
    
      
    ) dbt_internal_test