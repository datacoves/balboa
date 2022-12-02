
  create or replace  view BALBOA_STAGING.l1_account_usage.query_history
  
    
    
(
  
    "QUERY_ID" COMMENT $$$$, 
  
    "QUERY_TEXT" COMMENT $$$$, 
  
    "DATABASE_ID" COMMENT $$$$, 
  
    "DATABASE_NAME" COMMENT $$$$, 
  
    "SCHEMA_ID" COMMENT $$$$, 
  
    "SCHEMA_NAME" COMMENT $$$$, 
  
    "QUERY_TYPE" COMMENT $$$$, 
  
    "SESSION_ID" COMMENT $$$$, 
  
    "USER_NAME" COMMENT $$$$, 
  
    "ROLE_NAME" COMMENT $$$$, 
  
    "WAREHOUSE_ID" COMMENT $$$$, 
  
    "WAREHOUSE_NAME" COMMENT $$$$, 
  
    "WAREHOUSE_SIZE" COMMENT $$$$, 
  
    "WAREHOUSE_TYPE" COMMENT $$$$, 
  
    "CLUSTER_NUMBER" COMMENT $$$$, 
  
    "QUERY_TAG" COMMENT $$$$, 
  
    "EXECUTION_STATUS" COMMENT $$$$, 
  
    "ERROR_CODE" COMMENT $$$$, 
  
    "ERROR_MESSAGE" COMMENT $$$$, 
  
    "START_TIME" COMMENT $$$$, 
  
    "END_TIME" COMMENT $$$$, 
  
    "TOTAL_ELAPSED_TIME" COMMENT $$$$, 
  
    "BYTES_SCANNED" COMMENT $$$$, 
  
    "PERCENTAGE_SCANNED_FROM_CACHE" COMMENT $$$$, 
  
    "BYTES_WRITTEN" COMMENT $$$$, 
  
    "BYTES_WRITTEN_TO_RESULT" COMMENT $$$$, 
  
    "BYTES_READ_FROM_RESULT" COMMENT $$$$, 
  
    "ROWS_PRODUCED" COMMENT $$$$, 
  
    "ROWS_INSERTED" COMMENT $$$$, 
  
    "ROWS_UPDATED" COMMENT $$$$, 
  
    "ROWS_DELETED" COMMENT $$$$, 
  
    "ROWS_UNLOADED" COMMENT $$$$, 
  
    "BYTES_DELETED" COMMENT $$$$, 
  
    "PARTITIONS_SCANNED" COMMENT $$$$, 
  
    "PARTITIONS_TOTAL" COMMENT $$$$, 
  
    "BYTES_SPILLED_TO_LOCAL_STORAGE" COMMENT $$$$, 
  
    "BYTES_SPILLED_TO_REMOTE_STORAGE" COMMENT $$$$, 
  
    "BYTES_SENT_OVER_THE_NETWORK" COMMENT $$$$, 
  
    "COMPILATION_TIME" COMMENT $$$$, 
  
    "EXECUTION_TIME" COMMENT $$$$, 
  
    "QUEUED_PROVISIONING_TIME" COMMENT $$$$, 
  
    "QUEUED_REPAIR_TIME" COMMENT $$$$, 
  
    "QUEUED_OVERLOAD_TIME" COMMENT $$$$, 
  
    "TRANSACTION_BLOCKED_TIME" COMMENT $$$$, 
  
    "OUTBOUND_DATA_TRANSFER_CLOUD" COMMENT $$$$, 
  
    "OUTBOUND_DATA_TRANSFER_REGION" COMMENT $$$$, 
  
    "OUTBOUND_DATA_TRANSFER_BYTES" COMMENT $$$$, 
  
    "INBOUND_DATA_TRANSFER_CLOUD" COMMENT $$$$, 
  
    "INBOUND_DATA_TRANSFER_REGION" COMMENT $$$$, 
  
    "INBOUND_DATA_TRANSFER_BYTES" COMMENT $$$$, 
  
    "LIST_EXTERNAL_FILES_TIME" COMMENT $$$$, 
  
    "CREDITS_USED_CLOUD_SERVICES" COMMENT $$$$, 
  
    "RELEASE_VERSION" COMMENT $$$$, 
  
    "EXTERNAL_FUNCTION_TOTAL_INVOCATIONS" COMMENT $$$$, 
  
    "EXTERNAL_FUNCTION_TOTAL_SENT_ROWS" COMMENT $$$$, 
  
    "EXTERNAL_FUNCTION_TOTAL_RECEIVED_ROWS" COMMENT $$$$, 
  
    "EXTERNAL_FUNCTION_TOTAL_SENT_BYTES" COMMENT $$$$, 
  
    "EXTERNAL_FUNCTION_TOTAL_RECEIVED_BYTES" COMMENT $$$$, 
  
    "QUERY_LOAD_PERCENT" COMMENT $$$$, 
  
    "IS_CLIENT_GENERATED_STATEMENT" COMMENT $$$$
  
)

  copy grants as (
    with raw_source as (

    select *
    from SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY

),

final as (

    select
        "QUERY_ID" as query_id,
        "QUERY_TEXT" as query_text,
        "DATABASE_ID" as database_id,
        "DATABASE_NAME" as database_name,
        "SCHEMA_ID" as schema_id,
        "SCHEMA_NAME" as schema_name,
        "QUERY_TYPE" as query_type,
        "SESSION_ID" as session_id,
        "USER_NAME" as user_name,
        "ROLE_NAME" as role_name,
        "WAREHOUSE_ID" as warehouse_id,
        "WAREHOUSE_NAME" as warehouse_name,
        "WAREHOUSE_SIZE" as warehouse_size,
        "WAREHOUSE_TYPE" as warehouse_type,
        "CLUSTER_NUMBER" as cluster_number,
        "QUERY_TAG" as query_tag,
        "EXECUTION_STATUS" as execution_status,
        "ERROR_CODE" as error_code,
        "ERROR_MESSAGE" as error_message,
        "START_TIME" as start_time,
        "END_TIME" as end_time,
        "TOTAL_ELAPSED_TIME" as total_elapsed_time,
        "BYTES_SCANNED" as bytes_scanned,
        "PERCENTAGE_SCANNED_FROM_CACHE" as percentage_scanned_from_cache,
        "BYTES_WRITTEN" as bytes_written,
        "BYTES_WRITTEN_TO_RESULT" as bytes_written_to_result,
        "BYTES_READ_FROM_RESULT" as bytes_read_from_result,
        "ROWS_PRODUCED" as rows_produced,
        "ROWS_INSERTED" as rows_inserted,
        "ROWS_UPDATED" as rows_updated,
        "ROWS_DELETED" as rows_deleted,
        "ROWS_UNLOADED" as rows_unloaded,
        "BYTES_DELETED" as bytes_deleted,
        "PARTITIONS_SCANNED" as partitions_scanned,
        "PARTITIONS_TOTAL" as partitions_total,
        "BYTES_SPILLED_TO_LOCAL_STORAGE" as bytes_spilled_to_local_storage,
        "BYTES_SPILLED_TO_REMOTE_STORAGE" as bytes_spilled_to_remote_storage,
        "BYTES_SENT_OVER_THE_NETWORK" as bytes_sent_over_the_network,
        "COMPILATION_TIME" as compilation_time,
        "EXECUTION_TIME" as execution_time,
        "QUEUED_PROVISIONING_TIME" as queued_provisioning_time,
        "QUEUED_REPAIR_TIME" as queued_repair_time,
        "QUEUED_OVERLOAD_TIME" as queued_overload_time,
        "TRANSACTION_BLOCKED_TIME" as transaction_blocked_time,
        "OUTBOUND_DATA_TRANSFER_CLOUD" as outbound_data_transfer_cloud,
        "OUTBOUND_DATA_TRANSFER_REGION" as outbound_data_transfer_region,
        "OUTBOUND_DATA_TRANSFER_BYTES" as outbound_data_transfer_bytes,
        "INBOUND_DATA_TRANSFER_CLOUD" as inbound_data_transfer_cloud,
        "INBOUND_DATA_TRANSFER_REGION" as inbound_data_transfer_region,
        "INBOUND_DATA_TRANSFER_BYTES" as inbound_data_transfer_bytes,
        "LIST_EXTERNAL_FILES_TIME" as list_external_files_time,
        "CREDITS_USED_CLOUD_SERVICES" as credits_used_cloud_services,
        "RELEASE_VERSION" as release_version,
        "EXTERNAL_FUNCTION_TOTAL_INVOCATIONS" as external_function_total_invocations,
        "EXTERNAL_FUNCTION_TOTAL_SENT_ROWS" as external_function_total_sent_rows,
        "EXTERNAL_FUNCTION_TOTAL_RECEIVED_ROWS" as external_function_total_received_rows,
        "EXTERNAL_FUNCTION_TOTAL_SENT_BYTES" as external_function_total_sent_bytes,
        "EXTERNAL_FUNCTION_TOTAL_RECEIVED_BYTES" as external_function_total_received_bytes,
        "QUERY_LOAD_PERCENT" as query_load_percent,
        "IS_CLIENT_GENERATED_STATEMENT" as is_client_generated_statement

    from raw_source

)

select * from final
  );
