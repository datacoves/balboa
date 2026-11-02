{#
    Lists all "<base>_PR_*" databases, one per line, as
    "<name>,<created_on ISO 8601>,<last_query ISO 8601 or empty>".
    Used by the sweep step in workflow 30 (deploy to production): the step keeps
    only lines matching that pattern (dbt also prints log lines) and passes them
    to sweep_stale_pr_databases.py, which uses created_on for the max-age rule
    and last_query for the inactivity check.

    "<base>" is target.database -- in the deploy job DATACOVES__MAIN__DATABASE is
    the bare base name (no _PR_/_QA_TEMP suffix), so target.database already is
    the base to match against.

    last_query is the most recent query run with the database as session context
    (ACCOUNT_USAGE.QUERY_HISTORY, up to ~45 min latency) within the last
    lookback_days, excluding queries by excluded_user (the CI service user, so
    builds don't count as activity -- defaults to target.user, the Snowflake user
    the deploy job is connected as). Empty when there was no such query.

    Requires IMPORTED PRIVILEGES on the SNOWFLAKE database for the running role.

    Note: the "<base>_PR_*" pattern this macro matches against assumes PR-scoped
    databases keep that exact naming convention; a database renamed to not end
    in "_PR_<digits>" (e.g. a manually protected "..._KEEP" database) is
    correctly excluded by the downstream sweep script's id-extraction regex,
    confirmed against real account data during the deploy that validated this
    macro end to end (2026-11-02).

    To run:
    dbt run-operation show_pr_databases
    dbt run-operation show_pr_databases --args '{lookback_days: 15}'
#}

{%- macro show_pr_databases(lookback_days=15, excluded_user=none) -%}

    {%- set excluded_user = excluded_user or target.user -%}
    {%- set pattern = target.database ~ '_PR_%' -%}

    {% set show_sql %}
        SHOW DATABASES LIKE '{{ pattern }}'
    {% endset %}
    {% set results = run_query(show_sql) %}

    {% set activity_sql %}
        SELECT database_name, MAX(start_time) AS last_query
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE database_name LIKE '{{ pattern }}'
          AND start_time > DATEADD(day, -{{ lookback_days | int }}, CURRENT_TIMESTAMP())
          AND user_name <> '{{ excluded_user | upper | replace("'", "") }}'
        GROUP BY database_name
    {% endset %}
    {% set activity = run_query(activity_sql) %}

    {% set last_query_by_db = {} %}
    {% for row in activity %}
        {% do last_query_by_db.update({row[0]: row[1].isoformat()}) %}
    {% endfor %}

    {% for row in results %}
        {{ print(row['name'] ~ ',' ~ row['created_on'].isoformat() ~ ',' ~ last_query_by_db.get(row['name'], '')) }}
    {% endfor %}

{%- endmacro -%}
