{# Macro for finding differences between Permifrost and Snowflake #}
{# 
    Run using
    dbt run-operation objects_not_in_permifrost  --args '{permifrost_list: demo_db1,demo_db2, obj_type: databases}'
#}

{% macro objects_not_in_permifrost(obj_type, permifrost_list) %}
    {% if obj_type == "Warehouses" %}
        {% set permifrost_names = [] %}
        {% for wh in permifrost_list %}
            {% for k, v in wh.items() %}
                {%if k == "name"%}
                    {{ permifrost_names.append(v.lower()) }}
                {%endif%}
            {%endfor%}
        {%endfor%}
    {% else %}
        {% set permifrost_names = permifrost_list.lower().split(',') %}
    {% endif %}
    
    {% set objects_to_be_created = [] %}
    

    {% if obj_type == "Schemas"%}
        {% set permifrost_databases = [] %}
        {% set permifrost_schemas = [] %}
        {% for object in permifrost_names %}
            {% set schema_as_list = object.lower().split('.') %}
            {% set db = schema_as_list[0] %}            
            {% if db not in permifrost_databases %}
                {{ permifrost_databases.append(db) }}
            {% endif %}
        {% endfor %}

        {% set snowflake_objects_names = [] %}

        {% for db in permifrost_databases %}
            {% set showflake_schemas = run_query("show " ~ obj_type ~ " in database " ~ db) %}
            {% set snowflake_schemas_names = showflake_schemas.columns["name"].values() %}
            {% for schema_name in snowflake_schemas_names %}
                {{ snowflake_objects_names.append(db.upper()~"."~schema_name.upper()) }}
            {% endfor %}            
        {%endfor%}

    {% else %}
        {% set snowflake_objects = run_query("show " ~ obj_type) %}
        {% set snowflake_objects_names = snowflake_objects.columns["name"].values() %}
    {% endif %}

    {% if snowflake_objects_names %}
        {{ log(obj_type ~ " found in Snowflake that do not exist in Permifrost configs", true) }}
        {% for name_ in snowflake_objects_names %}
            {% if name_.lower() not in permifrost_names %}
                {{ log(name_, true) }}
            {% endif %}
        {% endfor %}

        {% for name_ in permifrost_names %}
            {% if name_.upper() not in snowflake_objects_names %}
                {% if obj_type == "Warehouses"%}
                    {% for wh in permifrost_list %}
                        {% for k, v in wh.items() %}
                            {%if k == "name" and v == name_%}
                                {{ objects_to_be_created.append(wh) }}
                            {%endif%}
                        {%endfor%}
                    {%endfor%}                    
                {% else %}
                    {{ objects_to_be_created.append(name_) }}
                {% endif %}                
            {% endif %}
        {% endfor %}
    {% endif %}

    {% if objects_to_be_created %}
        
        {% if obj_type == "Warehouses"%}
            {{ create_snowflake_warehouses(objects_to_be_created) }}
        {%endif%}
        {% if obj_type == "Schemas"%}
            {{ create_snowflake_schemas(objects_to_be_created) }}
        {%endif%}
        {% if obj_type == "Roles"%}
            {{ create_snowflake_roles(objects_to_be_created) }}
        {%endif%}
        {% if obj_type == "Databases"%}
            {{ create_snowflake_databases(objects_to_be_created) }}
        {%endif%}
    {% endif %}        
{% endmacro %}
