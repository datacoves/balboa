select *
from {{ metrics.calculate(
    [
        metric('sum_cases')
    ],
    grain='year',
    dimensions=['country','state'],

    where="country='United States'
        and state='California'",
    start_date='2020-01-01'
) }}
order by date_year desc


{#
        metric('sum_deaths'),
        metric('average_cases'),
        metric('average_deaths') #}
