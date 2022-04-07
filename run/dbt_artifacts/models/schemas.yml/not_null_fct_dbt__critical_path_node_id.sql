select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from staging_BALBOA.dbt_test__audit.not_null_fct_dbt__critical_path_node_id
    
      
    ) dbt_internal_test