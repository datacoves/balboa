{# Macro for finding differences in roles between Permifrost and Snowflake #}
{#
    Run using
    dbt run-operation snowflake_role_comparison  --args '{permifrost_list: demo_db1,demo_db2, dry_run: true}'
#}

{% macro snowflake_role_comparison(permifrost_role_list, dry_run = true) %}
    {{ print("Running as user: " ~ target.user )}}
    {{ print("Running as using target: " ~ target.name )}}
    {{ print('\n')}}

    {% if(permifrost_role_list |length == 0) %}
        {{ exceptions.raise_compiler_error("List of roles to compare is empty") }}
    {% else %}}
        {% set permifrost_roles = permifrost_role_list.upper().split(',') %}

        {% set roles_to_be_created = [] %}
        {% set roles_missing_in_permifrost = [] %}

        {% set roles_results = run_query("use role securityadmin; show roles;") %}
        {% set roles_in_snowflake = roles_results.columns["name"].values() %}

        {# We don't care about default snowflake schemas #}
        {% set excluded_roles = ["ORGADMIN","ACCOUNTADMIN","SYSADMIN","SECURITYADMIN","USERADMIN"] %}

        {% for role in permifrost_roles %}
            {% if  (role.upper() not in excluded_roles) and role.upper() not in roles_in_snowflake %}
                {{ roles_to_be_created.append(role) }}
            {% endif %}
        {% endfor %}

        {% for role in roles_in_snowflake %}
            {% if role.upper() not in permifrost_roles %}
                {{ roles_missing_in_permifrost.append(role) }}
            {% endif %}
        {% endfor %}

        {{ print('#######################################')}}
        {{ print('####### Roles not in Permifrost #######')}}
        {{ print('#######################################')}}

        {{ print('\n'.join(roles_missing_in_permifrost))}}
        {{ print('\n') }}

        {% if roles_to_be_created %}

            {{ print('#######################################')}}
            {{ print('####### Roles not in Snowflake #######')}}
            {{ print('#######################################')}}

            {{ print('\n'.join(roles_to_be_created))}}
            {{ print('\n') }}

            {% if dry_run == true %}
                {{ print('Roles not created during a dry_run')}}
            {% else %}
                {{ create_snowflake_roles(roles_to_be_created) }}
            {% endif %}

        {% else %}

            {{ print('=======================================')}}
            {{ print('Roles in Permifrost exist in Snowflake')}}
            {{ print('=======================================\n')}}

        {% endif %}
    {% endif %}

{% endmacro %}
