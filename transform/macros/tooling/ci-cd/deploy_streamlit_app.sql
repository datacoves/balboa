{# This macro creates a streamlit app. #}
{#
    To run:
    dbt run-operation deploy_streamlit_app --args "{app_path: '/config/workspace/visualize/streamlit/loans-example', app_file: loans.py}" -t prd
#}

{%- macro deploy_streamlit_app(
                app_path="/config/workspace/visualize/streamlit/loans-example",
                app_file="loans.py",
                app_stage="balboa_apps.resources.streamlit") -%}

    {% set create_sql %}
        PUT 'file://{{ app_path }}/{{ app_file }}'
            '@{{ app_stage }}'
            overwrite=true
            auto_compress=false;
    {% endset %}

    {% set results = run_query(create_sql) %}

    {{ print("Application deployed: " + results.columns[0].values()[0] ) }}

{%- endmacro -%}
