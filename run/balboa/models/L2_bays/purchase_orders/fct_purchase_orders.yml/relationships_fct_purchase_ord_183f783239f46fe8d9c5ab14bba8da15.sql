
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from BALBOA_STAGING.DBT_TEST__AUDIT.relationships_fct_purchase_ord_183f783239f46fe8d9c5ab14bba8da15
    
      
    ) dbt_internal_test