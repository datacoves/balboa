
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from BALBOA_STAGING.DBT_TEST__AUDIT.relationships_fct_invoices_d565d77ddbf9c8b01f5d2a0930feee8c
    
      
    ) dbt_internal_test