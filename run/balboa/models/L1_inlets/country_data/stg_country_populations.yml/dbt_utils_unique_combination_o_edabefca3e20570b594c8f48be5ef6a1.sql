select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from BALBOA_STAGING.DBT_TEST__AUDIT.dbt_utils_unique_combination_o_edabefca3e20570b594c8f48be5ef6a1
    
      
    ) dbt_internal_test