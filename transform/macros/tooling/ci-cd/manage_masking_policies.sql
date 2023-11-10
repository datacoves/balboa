#! /bin/bash

# Cause script to exit on error
{# This macro processes masking policies during ci/cd #}
{#
    To run:
    dbt run-operation manage_masking_policies -t prod
#}

{% macro manage_masking_policies() %}

    {{ log("Creating schemas for masking policies", true) }}
    {% do create_missing_schemas_with_masking_policy() %}

    {{ log("Unapplying masking policies", true) }}
    {% do snow_mask_reapply_policies('unapply') %}

    {{ log("Create masking policies", true) }}
    {% if target.name in ['prod', 'test'] %}
        {% do dbt_snow_mask.create_masking_policy(resource_type='sources') %}
    {% endif %}
    {% do dbt_snow_mask.create_masking_policy(resource_type='models') %}

    {{ log("Reapply masking policies", true) }}
    {% do snow_mask_reapply_policies('apply') %}

{% endmacro %}
