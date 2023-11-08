{# This macro exports Snowflake data to an external file on S3 #}
{#
    To run:
    dbt run-operation offload_table --args "{model_name: 'data_export', stage: 'DATACOVES_DB.ETL1.EXT_LINEITEM_STAGE'}" -t dev_xs
#}

{% macro offload_table(model_name, stage = "DATACOVES_DB.ETL1.EXT_LINEITEM_STAGE") %}

    {{ print("Exporting " + model_name) }}

    {% set copy_sql %}
        copy into @{{ stage }}/ng_test_{{ model_name }}/data_
        from (
            select
                L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE, L_SHIPINSTRUCT, L_SHIPMODE, L_COMMENT
            from DATACOVES_DB.ETL1.DC_TC2_LINEITEM
        )
        header = true
        overwrite = true
        max_file_size = 1073741824;
    {% endset %}

    {% set results = run_query(copy_sql) %}

    {{ print(results.columns[0].values()) }}

{% endmacro %}
