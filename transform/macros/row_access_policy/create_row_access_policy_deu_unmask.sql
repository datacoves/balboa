{% macro create_row_access_policy_region(node_database,node_schema) %}
{# 
  This should be run by dbt-snow-mask, but as this does not currently support row masking, 
  is is manually run for now.

    To run: 
    dbt run-operation create_row_access_policy_region --args '{node_database: prd_commercial_dw, node_schema: source_marketedge}'

  To apply to a table / view:
  alter [table/view] <table_name> add row access policy z_policy_row_region_de on [country code column]

  alter view MARKETEDGE_INTERNAL add row access policy z_policy_row_region on (ISO3_COUNTRY_CODE);

  Policy must be dropped from table / view in order to recreate it
  alter [table/view] <table_name> drop row access policy z_policy_row_region_de

  alter view MARKETEDGE_INTERNAL drop row access policy z_policy_row_region;

#}

{# Rows with removal comments should be removed when transitioning to the dbt-snow-mask approach. #}

  {% set create_policy_sql %} {# to remove #}
    use role securityadmin;
    create or replace row access policy {{node_database}}.{{node_schema}}.z_policy_row_region 
    as (country_code string) 
      returns boolean ->
          case 
            when is_role_in_session('Z_POLICY_ROW_REGION_ALL') then true
            when country_code = 'DEU' AND is_role_in_session('Z_POLICY_ROW_REGION_DE') then true
            else false
          end;
    grant apply on row access policy {{node_database}}.{{node_schema}}.z_policy_row_region to role transformer_dbt_prd;
  {% endset %} {# to remove #}
  {% do run_query(create_policy_sql) %} {# to remove #}
  {{ log("Created policy: " ~ create_policy_sql, info=true) }}
{% endmacro %}