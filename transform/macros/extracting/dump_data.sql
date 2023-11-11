{# This macro exports Snowflake data to an external file on S3 #}
{#
    To run:
    dbt run-operation offload_table --args "{model_name: 'personal_loans', stage: 'RAW.RAW.EXT_JSONFILES_STAGE'}" -t prd
#}

{% macro offload_table(model_name, stage = "DATACOVES_DB.ETL1.EXT_LINEITEM_STAGE") %}

    {{ print("Exporting " + model_name) }}

    {% set copy_sql %}
        copy into @{{ stage }}/ng_test_{{ model_name }}/data_
        from (
            select * from {{ ref(model_name) }}
        )
        header = true
        overwrite = true
        max_file_size = 1073741824;
    {% endset %}

    {% set results = run_query(copy_sql) %}

    {{ print(results.columns[0].values()) }}

{% endmacro %}
