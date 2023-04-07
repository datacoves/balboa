select *
from {{ metrics.calculate(
    [
        metric('sum_cases')
    ],
    grain='month',
    dimensions=['country','state'],

    start_date='2020-01-01'
) }}
order by 1 desc


{#
        metric('sum_deaths'),
        metric('average_cases'),
        metric('average_deaths') #}
