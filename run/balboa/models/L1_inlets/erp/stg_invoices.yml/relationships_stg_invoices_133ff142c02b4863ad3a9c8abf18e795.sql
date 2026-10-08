
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from BALBOA_STAGING.DBT_TEST__AUDIT.relationships_stg_invoices_133ff142c02b4863ad3a9c8abf18e795
    
      
    ) dbt_internal_test