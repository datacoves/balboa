
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from BALBOA_STAGING.DBT_TEST__AUDIT.relationships_stg_invoices__er_caa43c76779f6efc23202ef4076059d7
    
      
    ) dbt_internal_test