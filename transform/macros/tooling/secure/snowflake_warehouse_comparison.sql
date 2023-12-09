{# Macro for finding differences in roles between Permifrost and Snowflake #}
{#
    Run using
    dbt run-operation snowflake_warehouse_comparison  --args '{permifrost_list: demo_db1,demo_db2, dry_run: true}'
#}

{% macro snowflake_warehouse_comparison(permifrost_warehouse_list, dry_run = true) %}
    {{ print("Running as user: " ~ target.user )}}
    {{ print("Running as using target: " ~ target.name )}}
    {{ print('\n')}}

    {% set permifrost_warehouses = [] %}

    {% set warehouses_to_be_created = [] %}
    {% set warehouses_missing_in_permifrost = [] %}

    {% for wh in permifrost_warehouse_list %}
        {% for k, v in wh.items() %}
            {% if k == "name" %}
                {{ permifrost_warehouses.append(v.upper()) }}
            {% endif %}
        {% endfor %}
    {% endfor %}

    {% set warehouse_results = run_query("use role sysadmin; show warehouses;") %}
    {% set warehousess_in_snowflake = warehouse_results.columns["name"].values() %}

    {% for warehouse in permifrost_warehouse_list %}
        {% if warehouse['name'].upper() not in warehousess_in_snowflake %}
            {{ warehouses_to_be_created.append(warehouse) }}
        {% endif %}
    {% endfor %}

    {% for warehouse in warehousess_in_snowflake %}
        {% if warehouse.upper() not in permifrost_warehouses %}
            {{ warehouses_missing_in_permifrost.append(warehouse) }}
        {% endif %}
    {% endfor %}

    {{ print('############################################')}}
    {{ print('####### Warehouses not in Permifrost #######')}}
    {{ print('############################################')}}

    {{ print('\n'.join(warehouses_missing_in_permifrost))}}
    {{ print('\n') }}

    {% if warehouses_to_be_created %}

        {{ print('###########################################')}}
        {{ print('####### Warehouses not in Snowflake #######')}}
        {{ print('###########################################')}}

        {% for warehouse in warehouses_to_be_created %}
            {{ print(warehouse['name'].upper()) }}
        {% endfor %}

        {{ print('\n') }}

        {% if dry_run == true %}
            {{ print('Warehouses not created during a dry_run')}}
        {% else %}
            {{ create_snowflake_warehouses(warehouses_to_be_created) }}
        {% endif %}

    {% else %}

        {{ print('===========================================')}}
        {{ print('Warehouses in Permifrost exist in Snowflake')}}
        {{ print('===========================================\n')}}

    {% endif %}

{% endmacro %}
