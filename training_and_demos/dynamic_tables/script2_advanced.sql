-- =================================================================================
-- Create a stream on the dynamic table
-- This will track changes to the dynamic table
-- =================================================================================


-- drop stream balboa_dev.gomezn.dynamic_table_stream;

create or replace stream balboa_dev.gomezn.dynamic_table_stream
  on dynamic table balboa_dev.gomezn.stg_personal_loans;

select * from balboa_dev.gomezn.dynamic_table_stream;

-- Run this to clear out the stream
-- create or replace temp table empty_stream as select * from BALBOA_DEV.GOMEZN.dynamic_table_stream;



-- =================================================================================
-- Create empty target table to hold the errors
-- =================================================================================

create table balboa_dev.gomezn.errors_table as
(
    select * from balboa_dev.gomezn.stg_test_failures
    where 1=0
);



-- =================================================================================
-- Create a task that should run when the stream has data
-- =================================================================================

-- drop task if exists balboa_dev.gomezn.capture_errors_task;

create task balboa_dev.gomezn.capture_errors_task
  target_completion_interval='1 minutes'
  when system$stream_has_data('dynamic_table_stream')
  as
    begin
        -- Truncates the errors_table
        delete from balboa_dev.gomezn.errors_table;

        -- Inserts record from the stg_test_failures view that have failures
        insert into balboa_dev.gomezn.errors_table
            select * from balboa_dev.gomezn.stg_test_failures where count_failed > 0;

        -- Clear stream on the dynamic table
        create or replace temp table empty_stream as select * from balboa_dev.gomezn.dynamic_table_stream;
    end;

-- Starts the task
alter task balboa_dev.gomezn.capture_errors_task resume;

-- alter task balboa_dev.gomezn.capture_errors_task suspend;

-- =================================================================================
-- Show each time the task has run
-- =================================================================================

select *
  from table(information_schema.task_history(
    scheduled_time_range_start=>dateadd('hour',-1,current_timestamp()),
    result_limit => 10,
    task_name=>'capture_errors_task'));

-- Errors Table now has a record
select * from balboa_dev.gomezn.errors_table;


-- =================================================================================
-- Track changes on the errors_table
-- =================================================================================

alter table balboa_dev.gomezn.errors_table set change_tracking = true;


-- =================================================================================
-- Create an alert when there are errors in the errors_table
-- =================================================================================

create or replace alert balboa_dev.gomezn.error_alert
  if( exists(
    select '<p><strong>test name:</strong> ' || test_name ||
           '</p><p><strong>exceptions:</strong> ' || count_failed ||
           '</p><p><strong>model names:</strong> ' || tested_models || '</p><hr>'
           as error_message
    from balboa_dev.gomezn.errors_table
  ))
  then
    begin
      let result_str varchar;
      (select array_to_string(array_agg( error_message )::array, '') into :result_str
        from (
          select *
            from table(result_scan(snowflake.alert.get_condition_query_uuid()))
            limit 20
        )
      );

        call system$send_snowflake_notification(
        snowflake.notification.text_html(
            --- email message
             '<h2>Exceptions were detected after dynamic table update</h2><br> ' || :result_str
            ),
        snowflake.notification.email_integration_config(
            --- notification integration
            'send_email_notifications',

            --- email subject
            'alert: errors detected on dynamic table refresh',

            --- emails which must be verified snowflake emails
            array_construct('gomezn@datacoves.com','gomezn@datacov.es'),
            --- cc emails
            null,
            --- bcc emails
            null
            )
        );
    end;


-- Starts the alert
alter alert balboa_dev.gomezn.error_alert resume;


-- Monitor if the alert ran
select
    to_char(convert_timezone('america/los_angeles', scheduled_time), 'yyyy-mm-dd at hh:mi:ss') as scheduled_time,
    -- replace above with your timezone
    name,
    state,
    sql_error_message,      -- in case an alert itself failed
    timediff(second, scheduled_time, completed_time) as duration_in_s,
    schema_name
from
    table ( balboa_dev.information_schema.alert_history())
where
    state != 'scheduled'
    and schema_name = 'gomezn'
order by
    scheduled_time desc
limit
    20;
