
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from BALBOA.DBT_TEST__AUDIT.unique_seed_purchase_orders_po_id
    
      
    ) dbt_internal_test