{# macros/tooling/create_error_capture_objects.sql #}
{#
  One-time, idempotent setup for the serverless error-capture pipeline whose TASK and
  ALERT are managed by snowcap (secure/snowcap/resources/tasks.yml + alerts.yml).

  It creates the two objects snowcap does not manage:
    - errors_table:         empty table shaped from stg_test_failures, that
                            capture_errors_task truncates and refills
    - dynamic_table_stream: the stream on stg_personal_loans whose WHEN
                            system$stream_has_data(...) triggers the task

  Both are built in the schema where the referenced models live, so the target
  decides where they land (e.g. your gomezn dev schema). Run before applying snowcap:

    dbt run-operation create_error_capture_objects

  Re-running is safe: both use CREATE ... IF NOT EXISTS. This replaces the manual
  setup in training_and_demos/dynamic_tables/script2_advanced.sql.
#}

{% macro create_error_capture_objects() %}
  {% if execute %}
    {% set failures = ref('stg_test_failures') %}
    {% set loans = ref('stg_personal_loans') %}

    {% set errors_table = api.Relation.create(
        database=failures.database, schema=failures.schema, identifier='errors_table') %}
    {% set stream = api.Relation.create(
        database=loans.database, schema=loans.schema, identifier='dynamic_table_stream') %}

    {{ log("Creating error-capture table: " ~ errors_table, info=true) }}
    {% do run_query("create table if not exists " ~ errors_table ~ " as select * from " ~ failures ~ " where 1=0") %}
    {% do run_query("alter table " ~ errors_table ~ " set change_tracking = true") %}

    {{ log("Creating trigger stream: " ~ stream, info=true) }}
    {% do run_query("create stream if not exists " ~ stream ~ " on dynamic table " ~ loans) %}
  {% endif %}
{% endmacro %}
