{# This macro drops every database (along it's schemas), warehouse and role  #}
{# Except the default objects that Snowflake provides #}
{#
    To run: 
    dbt run-operation drop_snowflake_objects
#}

{%- macro drop_snowflake_objects() -%}    
    {% set snowflake_objects_types = [
        {
            "type":"database",
            "owner":"TRANSFORMER_DBT_PRD",
            "defaults":["SNOWFLAKE", "SNOWFLAKE_SAMPLE_DATA"]
        },
        {
            "type":"warehouse",
            "owner": "SYSADMIN",
            "defaults":["COMPUTE_WH"]
        },
        {
            "type":"role",
            "owner":"ACCOUNTADMIN",
            "defaults":["ORGADMIN", "PUBLIC", "ACCOUNTADMIN", "SECURITYADMIN", "SYSADMIN", "USERADMIN"]
        }
    ] %}
    {% for obj in snowflake_objects_types %}
        {% set show_obj_sql %}
            use role {{obj["owner"]}};
            show {{obj["type"]}}s
        {% endset %}
        {% set snowflake_objects = run_query(show_obj_sql) %}
        {% set snowflake_objects_names = snowflake_objects.columns["name"].values() %}        
        {% for object in snowflake_objects_names %}
            {% if object not in obj["defaults"] %}
                {% set drop_obj_sql %}
                        use role {{obj["owner"]}};
                        drop {{obj["type"]}} {{object}};
                {% endset %}
                {% do run_query(drop_obj_sql) %}
                {{ log(obj["type"] ~ " " ~object~" dropped", info=true) }}
            {% endif %}
        {% endfor %}        
    {% endfor %}
{%- endmacro -%}
