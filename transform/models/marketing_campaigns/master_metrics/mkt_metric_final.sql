{% set dimensions_list = metric_prep() %}

{% set metric_models = dbt_utils.get_relations_by_pattern(
    schema_pattern='joe',
    table_pattern='metric_mkt_%',
    database='balboa_dev'
) %}

{{ dbt_utils.union_relations(
    relations=metric_models
) }}