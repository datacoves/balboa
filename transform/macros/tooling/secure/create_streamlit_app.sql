{# This macro creates a streamlit app. #}
{#
    To run:
    dbt run-operation create_streamlit_app --args '{app_name: loans, app_main_file: loans.py, grant_to_roles: [analyst]}' -t prd
#}



{%- macro create_streamlit_app(
                app_name,
                app_main_file,
                grant_to_roles,
                app_schema="balboa_apps.resources",
                app_stage="balboa_apps.resources.streamlit",
                app_warehouse="wh_transforming") -%}

    {% set fully_qualified_app_name = app_schema + '.' + app_name %}

    {% set create_sql %}
        create streamlit if not exists {{ fully_qualified_app_name }}
            root_location = '@{{ app_stage }}'
            main_file = '/{{ app_main_file }}'
            query_warehouse = '{{ app_warehouse }}';
    {% endset %}

    {% set results = run_query(create_sql) %}

    {{ print(results.columns[0].values() ) }}

    {% set grant_sql %}
        {% for role in grant_to_roles %}
            grant usage on streamlit {{ fully_qualified_app_name }} to role {{ role }};
        {% endfor %}
    {% endset %}

    {% set results = run_query(grant_sql) %}

    {{ print(results.columns[0].values() ) }}

{%- endmacro -%}
