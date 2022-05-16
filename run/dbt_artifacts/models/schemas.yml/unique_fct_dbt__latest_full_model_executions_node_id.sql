select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from staging_BALBOA.dbt_test__audit.unique_fct_dbt__latest_full_model_executions_node_id
    
      
    ) dbt_internal_test