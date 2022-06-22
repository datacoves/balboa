
  create or replace  view staging_BALBOA.source_account_usage.query_history
  
    
    
(
  
    
      QUERY_ID
    
    , 
  
    
      QUERY_TEXT
    
    , 
  
    
      DATABASE_ID
    
    , 
  
    
      DATABASE_NAME
    
    , 
  
    
      SCHEMA_ID
    
    , 
  
    
      SCHEMA_NAME
    
    , 
  
    
      QUERY_TYPE
    
    , 
  
    
      SESSION_ID
    
    , 
  
    
      USER_NAME
    
    , 
  
    
      ROLE_NAME
    
    , 
  
    
      WAREHOUSE_ID
    
    , 
  
    
      WAREHOUSE_NAME
    
    , 
  
    
      WAREHOUSE_SIZE
    
    , 
  
    
      WAREHOUSE_TYPE
    
    , 
  
    
      CLUSTER_NUMBER
    
    , 
  
    
      QUERY_TAG
    
    , 
  
    
      EXECUTION_STATUS
    
    , 
  
    
      ERROR_CODE
    
    , 
  
    
      ERROR_MESSAGE
    
    , 
  
    
      START_TIME
    
    , 
  
    
      END_TIME
    
    , 
  
    
      TOTAL_ELAPSED_TIME
    
    , 
  
    
      BYTES_SCANNED
    
    , 
  
    
      PERCENTAGE_SCANNED_FROM_CACHE
    
    , 
  
    
      BYTES_WRITTEN
    
    , 
  
    
      BYTES_WRITTEN_TO_RESULT
    
    , 
  
    
      BYTES_READ_FROM_RESULT
    
    , 
  
    
      ROWS_PRODUCED
    
    , 
  
    
      ROWS_INSERTED
    
    , 
  
    
      ROWS_UPDATED
    
    , 
  
    
      ROWS_DELETED
    
    , 
  
    
      ROWS_UNLOADED
    
    , 
  
    
      BYTES_DELETED
    
    , 
  
    
      PARTITIONS_SCANNED
    
    , 
  
    
      PARTITIONS_TOTAL
    
    , 
  
    
      BYTES_SPILLED_TO_LOCAL_STORAGE
    
    , 
  
    
      BYTES_SPILLED_TO_REMOTE_STORAGE
    
    , 
  
    
      BYTES_SENT_OVER_THE_NETWORK
    
    , 
  
    
      COMPILATION_TIME
    
    , 
  
    
      EXECUTION_TIME
    
    , 
  
    
      QUEUED_PROVISIONING_TIME
    
    , 
  
    
      QUEUED_REPAIR_TIME
    
    , 
  
    
      QUEUED_OVERLOAD_TIME
    
    , 
  
    
      TRANSACTION_BLOCKED_TIME
    
    , 
  
    
      OUTBOUND_DATA_TRANSFER_CLOUD
    
    , 
  
    
      OUTBOUND_DATA_TRANSFER_REGION
    
    , 
  
    
      OUTBOUND_DATA_TRANSFER_BYTES
    
    , 
  
    
      INBOUND_DATA_TRANSFER_CLOUD
    
    , 
  
    
      INBOUND_DATA_TRANSFER_REGION
    
    , 
  
    
      INBOUND_DATA_TRANSFER_BYTES
    
    , 
  
    
      LIST_EXTERNAL_FILES_TIME
    
    , 
  
    
      CREDITS_USED_CLOUD_SERVICES
    
    , 
  
    
      RELEASE_VERSION
    
    , 
  
    
      EXTERNAL_FUNCTION_TOTAL_INVOCATIONS
    
    , 
  
    
      EXTERNAL_FUNCTION_TOTAL_SENT_ROWS
    
    , 
  
    
      EXTERNAL_FUNCTION_TOTAL_RECEIVED_ROWS
    
    , 
  
    
      EXTERNAL_FUNCTION_TOTAL_SENT_BYTES
    
    , 
  
    
      EXTERNAL_FUNCTION_TOTAL_RECEIVED_BYTES
    
    , 
  
    
      QUERY_LOAD_PERCENT
    
    , 
  
    
      IS_CLIENT_GENERATED_STATEMENT
    
    
  
)

  copy grants as (
    with raw_source as (

    select *
    from snowflake.account_usage.QUERY_HISTORY

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
