select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from BALBOA_STAGING.DBT_TEST__AUDIT.dbt_utils_unique_combination_o_5781045f56e3107fdb5526606ef317cf
    
      
    ) dbt_internal_test