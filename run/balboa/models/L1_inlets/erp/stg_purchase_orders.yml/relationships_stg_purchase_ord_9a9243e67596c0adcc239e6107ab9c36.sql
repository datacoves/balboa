
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from BALBOA_STAGING.DBT_TEST__AUDIT.relationships_stg_purchase_ord_9a9243e67596c0adcc239e6107ab9c36
    
      
    ) dbt_internal_test