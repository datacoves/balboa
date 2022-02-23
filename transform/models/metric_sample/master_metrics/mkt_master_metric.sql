{% set  dimension_list = [
    'CAMPAIGN_NAME'
    , 'CAMPAIGN_EXECUTION_DATETIME'
    , 'CAMPAIGN_STATUS'
    , 'CAMPAIGN_TAG'
    , 'TARGET_LIST_NAME'
    , 'METRIC_NAME'
    , 'METRIC_VALUE'
] %}

{% set metric_models = dbt_utils.get_relations_by_pattern(
    schema_pattern='joe',
    table_pattern='metric_mkt_%',
    database='balboa_dev'
) %}


{{ dbt_utils.union_relations(
    relations=metric_models,
    include=dimension_list 
) }}