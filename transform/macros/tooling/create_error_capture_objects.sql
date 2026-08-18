{# macros/tooling/create_error_capture_objects.sql #}
{#
  One-time, idempotent setup for the serverless error-capture pipeline whose TASK,
  ALERT, and trigger STREAM are managed by snowcap (secure/snowcap/resources/
  tasks.yml, alerts.yml, streams.yml).

  It creates the one object snowcap does not manage: errors_table, an empty table
  shaped from stg_test_failures that capture_errors_task truncates and refills.
  (snowcap has no CREATE TABLE AS SELECT, so this stays in dbt where the shape is
  derived from the model; the trigger stream is now a snowcap DynamicTableStream.)

  Built in the schema where stg_test_failures lives, so the target decides where it
  lands (e.g. your gomezn dev schema). Run before applying snowcap:

    dbt run-operation create_error_capture_objects

  Re-running is safe: CREATE TABLE IF NOT EXISTS. This replaces the manual setup in
  training_and_demos/dynamic_tables/script2_advanced.sql.
#}

{% macro create_error_capture_objects() %}
  {% if execute %}
    {% set failures = ref('stg_test_failures') %}
    {% set errors_table = api.Relation.create(
        database=failures.database, schema=failures.schema, identifier='errors_table') %}

    {{ log("Creating error-capture table: " ~ errors_table, info=true) }}
    {% do run_query("create table if not exists " ~ errors_table ~ " as select * from " ~ failures ~ " where 1=0") %}
    {% do run_query("alter table " ~ errors_table ~ " set change_tracking = true") %}
  {% endif %}
{% endmacro %}
