{# Macro for creating a list of Warehouses in Snowflake. #}
{# 
    Run using
    dbt run-operation create_snowflake_warehouses  --args 'objects_to_be_created'
#}

{% macro create_snowflake_warehouses(objects_to_be_created) %}
    {% for wh in objects_to_be_created %}        
        
            {% set create_wh_sql %}
                use role sysadmin;
                create warehouse {{ wh["name"] }}
                {% if "parameters" in wh %}
                    with 
                    {% for k, v in wh["parameters"].items() %}
                        {% if k == "size" %}
                            WAREHOUSE_SIZE="{{v}}"
                        {% else %}
                            {{k.upper()}}={{v}}
                        {% endif %}
                        
                    {% endfor %}
                {% endif %}
                ;
            {% endset %}

            {% do run_query(create_wh_sql) %}
            {{ log("Warehouse "~wh["name"]~" created", info=true) }}


            
        
    {% endfor %}
{% endmacro %}