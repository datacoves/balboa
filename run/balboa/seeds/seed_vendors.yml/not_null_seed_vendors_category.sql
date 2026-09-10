
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from BALBOA.DBT_TEST__AUDIT.not_null_seed_vendors_category
    
      
    ) dbt_internal_test