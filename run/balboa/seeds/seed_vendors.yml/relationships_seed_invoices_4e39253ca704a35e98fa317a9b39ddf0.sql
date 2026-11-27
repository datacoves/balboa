
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from BALBOA.DBT_TEST__AUDIT.relationships_seed_invoices_4e39253ca704a35e98fa317a9b39ddf0
    
      
    ) dbt_internal_test