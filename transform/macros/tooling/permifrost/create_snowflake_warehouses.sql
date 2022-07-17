{# Macro for creating a list of Warehouses in Snowflake. #}
{# 
    Run using
    dbt run-operation create_snowflake_warehouses  --args "warehouses_to_be_created: 
        [{'name': 'wh_developer_emea', 'parameters': {'size': 'x-small', 'auto_suspend': 600, 'auto_resume': True, 'initially_suspended': True}]}"
#}

{% macro create_snowflake_warehouses(warehouses_to_be_created) %}
    
    {% for wh in warehouses_to_be_created %}        
        
            {% set create_wh_sql %}
                use role sysadmin;
                create warehouse {{ wh["name"] }}
                {% if "parameters" in wh -%}
                    with 
                    {% for k, v in wh["parameters"].items() -%}
                        {% if k == "size" -%}
                            WAREHOUSE_SIZE="{{v}}"
                        {% else -%}
                            {{k.upper()}}={{v}}
                        {% endif -%}
                    {%- endfor -%}
                {%- endif -%}
                ;
            {%- endset -%}

            {% do run_query(create_wh_sql) %}
            {{ print("Warehouse " ~ wh["name"] ~ " created") }}

    {% endfor %}
{% endmacro %}