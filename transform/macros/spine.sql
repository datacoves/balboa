{% macro trailing_edge_month() %}
    {% set as_of = var('as_of') %}
    date_trunc('month', dateadd(month, -1, to_date('{{ as_of }}')))
{% endmacro %}

{% macro procurement_month_spine() %}
    {% set trailing_edge = trailing_edge_month() %}

    select
        cast(
            dateadd(month, seq4(), dateadd(month, -23, {{ trailing_edge }}))
            as date
        ) as month_start
    from table(generator(rowcount => 24))
{% endmacro %}
