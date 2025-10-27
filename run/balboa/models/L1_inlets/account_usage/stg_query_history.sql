
  create or replace   view BALBOA_STAGING.L1_ACCOUNT_USAGE.stg_query_history
  
    
    
(
  
    "QUERY_ID" COMMENT $$A unique identifier assigned to the query$$, 
  
    "QUERY_TEXT" COMMENT $$The SQL text of the executed query$$, 
  
    "DATABASE_ID" COMMENT $$The unique identifier of the database where the query was executed$$, 
  
    "DATABASE_NAME" COMMENT $$The name of the database where the query was executed$$, 
  
    "SCHEMA_ID" COMMENT $$The unique identifier of the schema where the query was executed$$, 
  
    "SCHEMA_NAME" COMMENT $$The name of the schema where the query was executed$$, 
  
    "QUERY_TYPE" COMMENT $$The type of query executed$$, 
  
    "SESSION_ID" COMMENT $$A unique identifier for the session in which the query was executed$$, 
  
    "USER_NAME" COMMENT $$The name of the user who executed the query$$, 
  
    "ROLE_NAME" COMMENT $$The name of the role associated with the user who executed the query$$, 
  
    "WAREHOUSE_ID" COMMENT $$The unique identifier of the warehouse where the query was executed$$, 
  
    "WAREHOUSE_NAME" COMMENT $$The name of the warehouse where the query was executed$$, 
  
    "WAREHOUSE_SIZE" COMMENT $$The size of the warehouse where the query was executed$$, 
  
    "WAREHOUSE_TYPE" COMMENT $$The type of warehouse where the query was executed$$, 
  
    "CLUSTER_NUMBER" COMMENT $$The number of the cluster where the query was executed$$, 
  
    "QUERY_TAG" COMMENT $$The tag assigned to the query$$, 
  
    "EXECUTION_STATUS" COMMENT $$The status of the query execution$$, 
  
    "ERROR_CODE" COMMENT $$The error code if the query execution resulted in an error$$, 
  
    "ERROR_MESSAGE" COMMENT $$The error message if the query execution resulted in an error$$, 
  
    "START_TIME" COMMENT $$The start time of the query execution$$, 
  
    "END_TIME" COMMENT $$The end time of the query execution$$, 
  
    "TOTAL_ELAPSED_TIME" COMMENT $$The total time elapsed during the query execution$$, 
  
    "BYTES_SCANNED" COMMENT $$The amount of data in bytes scanned by the query$$, 
  
    "PERCENTAGE_SCANNED_FROM_CACHE" COMMENT $$The percentage of data scanned from cache during the query execution$$, 
  
    "BYTES_WRITTEN" COMMENT $$The amount of data in bytes written by the query$$, 
  
    "BYTES_WRITTEN_TO_RESULT" COMMENT $$The amount of data in bytes written to the query result$$, 
  
    "BYTES_READ_FROM_RESULT" COMMENT $$The amount of data in bytes read from the query result$$, 
  
    "ROWS_PRODUCED" COMMENT $$The number of rows produced by the query$$, 
  
    "ROWS_INSERTED" COMMENT $$The number of rows inserted by the query$$, 
  
    "ROWS_UPDATED" COMMENT $$The number of rows updated by the query$$, 
  
    "ROWS_DELETED" COMMENT $$The number of rows deleted by the query$$, 
  
    "ROWS_UNLOADED" COMMENT $$The number of rows unloaded by the query$$, 
  
    "BYTES_DELETED" COMMENT $$The amount of data in bytes deleted by the query$$, 
  
    "PARTITIONS_SCANNED" COMMENT $$The number of partitions scanned by the query$$, 
  
    "PARTITIONS_TOTAL" COMMENT $$The total number of partitions processed by the query$$, 
  
    "BYTES_SPILLED_TO_LOCAL_STORAGE" COMMENT $$The amount of data in bytes spilled to local storage during query execution$$, 
  
    "BYTES_SPILLED_TO_REMOTE_STORAGE" COMMENT $$The amount of data in bytes spilled to remote storage during query execution$$, 
  
    "BYTES_SENT_OVER_THE_NETWORK" COMMENT $$The amount of data in bytes sent over the network during query execution$$, 
  
    "COMPILATION_TIME" COMMENT $$The time taken for query compilation$$, 
  
    "EXECUTION_TIME" COMMENT $$The time taken for query execution$$, 
  
    "QUEUED_PROVISIONING_TIME" COMMENT $$The time taken for warehouse provisioning$$, 
  
    "QUEUED_REPAIR_TIME" COMMENT $$The time taken for warehouse repair$$, 
  
    "QUEUED_OVERLOAD_TIME" COMMENT $$The time taken for warehouse overload$$, 
  
    "TRANSACTION_BLOCKED_TIME" COMMENT $$The time taken for transaction blocking$$, 
  
    "OUTBOUND_DATA_TRANSFER_CLOUD" COMMENT $$The cloud provider for outbound data transfer$$, 
  
    "OUTBOUND_DATA_TRANSFER_REGION" COMMENT $$The region for outbound data transfer$$, 
  
    "OUTBOUND_DATA_TRANSFER_BYTES" COMMENT $$The amount of data in bytes transferred outbound$$, 
  
    "INBOUND_DATA_TRANSFER_CLOUD" COMMENT $$The cloud provider for inbound data transfer$$, 
  
    "INBOUND_DATA_TRANSFER_REGION" COMMENT $$The region for inbound data transfer$$, 
  
    "INBOUND_DATA_TRANSFER_BYTES" COMMENT $$The amount of data in bytes transferred inbound$$, 
  
    "LIST_EXTERNAL_FILES_TIME" COMMENT $$The time taken to list external files$$, 
  
    "CREDITS_USED_CLOUD_SERVICES" COMMENT $$The amount of credits used for cloud services$$, 
  
    "RELEASE_VERSION" COMMENT $$The release version of the query engine used$$, 
  
    "EXTERNAL_FUNCTION_TOTAL_INVOCATIONS" COMMENT $$The total number of invocations of external functions$$, 
  
    "EXTERNAL_FUNCTION_TOTAL_SENT_ROWS" COMMENT $$The total number of rows sent to external functions$$, 
  
    "EXTERNAL_FUNCTION_TOTAL_RECEIVED_ROWS" COMMENT $$The total number of rows received from external functions$$, 
  
    "EXTERNAL_FUNCTION_TOTAL_SENT_BYTES" COMMENT $$The amount of data in bytes sent to external functions$$, 
  
    "EXTERNAL_FUNCTION_TOTAL_RECEIVED_BYTES" COMMENT $$The amount of data in bytes received from external functions$$, 
  
    "QUERY_LOAD_PERCENT" COMMENT $$The percentage of query load on the system$$, 
  
    "IS_CLIENT_GENERATED_STATEMENT" COMMENT $$Whether the statement is a client-generated statement or not$$
  
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

