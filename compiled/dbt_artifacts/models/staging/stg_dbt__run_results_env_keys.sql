with base as (

    select *
    from 
    
        BALBOA.source_dbt_artifacts.stg_dbt__artifacts
    


),

run_results as (

    select *
    from base
    where artifact_type = 'run_results.json'

),

dbt_run as (

    select *
    from run_results
    where data:args:which = 'run'

),

env_keys as (

    select distinct env.key
    from dbt_run,
        lateral flatten(input => data:metadata:env) as env
    -- Sort results to ensure things are deterministic
    order by 1

)

select * from env_keys