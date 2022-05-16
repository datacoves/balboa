
  create or replace  view staging_BALBOA.source_dbt_artifacts.stg_dbt__run_results_env_keys 
  
  copy grants as (
    with base as (

    select *
    from source_dbt_artifacts.stg_dbt__run_results

),

dbt_run as (

    select *
    from base
    where execution_command = 'run'

),

env_keys as (

    select distinct env_key.key
    from dbt_run,
        lateral flatten(input => env) as env_key
    -- Sort results to ensure things are deterministic
    order by 1

)

select * from env_keys
  );
