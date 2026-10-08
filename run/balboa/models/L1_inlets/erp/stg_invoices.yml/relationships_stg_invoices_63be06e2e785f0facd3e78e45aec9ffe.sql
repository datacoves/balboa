
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from BALBOA_STAGING.DBT_TEST__AUDIT.relationships_stg_invoices_63be06e2e785f0facd3e78e45aec9ffe
    
      
    ) dbt_internal_test