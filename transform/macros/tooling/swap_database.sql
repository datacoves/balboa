{# This macro swaps two databases, for use in blue/green runs. #}
{#
    To run: 
    dbt run-operation swap_database --args "{db1: dev_commercial_dw2, db2: dev_commercial_dw}"
#}

{%- macro swap_database(db1, db2) -%}
    {% set swap_db_sql %}
        alter database {{ db1 }} swap with {{ db2 }};
    {% endset %}

    {% do run_query(swap_db_sql) %}
    {{ log("Swapped database " ~ db1 ~ " with " ~ db2, info=true) }}

{%- endmacro -%}
