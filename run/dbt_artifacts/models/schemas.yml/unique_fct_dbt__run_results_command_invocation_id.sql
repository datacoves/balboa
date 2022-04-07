select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from staging_BALBOA.dbt_test__audit.unique_fct_dbt__run_results_command_invocation_id
    
      
    ) dbt_internal_test