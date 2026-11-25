
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from BALBOA_STAGING.DBT_TEST__AUDIT.relationships_stg_purchase_ord_948afb0124c3285fb92ca85632f244b1
    
      
    ) dbt_internal_test