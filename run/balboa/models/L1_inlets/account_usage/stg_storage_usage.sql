
  create or replace   view BALBOA_STAGING.L1_ACCOUNT_USAGE.stg_storage_usage
  
    
    
(
  
    "USAGE_DATE" COMMENT $$The date when storage usage was recorded$$, 
  
    "STORAGE_BYTES" COMMENT $$The total amount of data in bytes stored$$, 
  
    "STAGE_BYTES" COMMENT $$The amount of data in bytes stored in the staging area$$, 
  
    "FAILSAFE_BYTES" COMMENT $$The amount of data in bytes stored in the failsafe area$$
  
)

  copy grants
  
  
  as (
    with raw_source as (

    select *
    from SNOWFLAKE.ACCOUNT_USAGE.STORAGE_USAGE

),

final as (

    select
        "USAGE_DATE" as usage_date,
        "STORAGE_BYTES" as storage_bytes,
        "STAGE_BYTES" as stage_bytes,
        "FAILSAFE_BYTES" as failsafe_bytes

    from raw_source

)

select * from final
  );

