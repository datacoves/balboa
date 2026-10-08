
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from BALBOA_STAGING.DBT_TEST__AUDIT.relationships_fct_invoices_12f6bd1b5b44a951648dcccd7f972f6e
    
      
    ) dbt_internal_test