{# This macro grants access to a database #}
{#
    To run:
    dbt run-operation grant_db_usage --args 'db_name: my_db'
#}
{#
    Restores the read grants that a blue-green swap cannot carry over.

    `dbt-coves blue-green` rebuilds grants on the staging database before the
    swap by reading them off production and re-issuing them: database-level
    grants via `show grants on database`, schema-level grants via `show grants
    on schema` (CloneDB.clone_database_grants / clone_database_schemas). Both
    of those come back correctly.

    Inherited grants do not. `show grants on database <db>` returns only the
    privileges held on the database object itself -- ownership and usage -- and
    never the inherited grants on the tables, views and dynamic tables inside
    it. Those are visible only from the grantee side, via `show grants to role
    z_tables_views__select`. Since the copy loop cannot see them, it cannot
    re-issue them, and the swap leaves the new database with usage but no
    select. That is why analysts lost read access to every table and view in
    BALBOA after the 2026-09-09 run.

    dbt-coves does not call this macro, so it has to be invoked explicitly
    after the swap. The grants below must match roles__base.yml in the snowcap
    config, so the next `snowcap apply` sees them as already in place rather
    than as drift.

    INHERITED modifies the privilege, not the object, and ALL is still
    required: `grant inherited select on all tables in database <db>`. The
    snowcap config spells the same grant as "inherited tables in database
    <db>", which is its own DSL and not valid SQL. Requires
    FEATURE_RBAC_INHERITED_GRANTS, set in snowcap's account.yml.
#}

{%- macro grant_db_usage(db_name) -%}
    {% set db_usage_role_prefix = var("db_usage_role_prefix") %}
    {% set tables_views_select_role = var("tables_views_select_role") %}

    {% set apply_db_grants_sql %}
        grant usage on database {{ db_name }} to role {{ db_usage_role_prefix }}{{ db_name }};
        grant usage on database {{ db_name }} to role useradmin;

        grant inherited select on all tables in database {{ db_name }} to role {{ tables_views_select_role }};
        grant inherited select on all dynamic tables in database {{ db_name }} to role {{ tables_views_select_role }};
        grant inherited select on all views in database {{ db_name }} to role {{ tables_views_select_role }};
    {% endset %}
    {% do run_query(apply_db_grants_sql) %}

    {{ log("Applied usage and select grants on Database: " ~ db_name, info=true) }}

{%- endmacro -%}
