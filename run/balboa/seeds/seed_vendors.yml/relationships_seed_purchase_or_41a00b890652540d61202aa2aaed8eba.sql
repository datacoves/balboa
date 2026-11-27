
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from BALBOA.DBT_TEST__AUDIT.relationships_seed_purchase_or_41a00b890652540d61202aa2aaed8eba
    
      
    ) dbt_internal_test