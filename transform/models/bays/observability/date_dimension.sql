-- generates a day row for 730 days
with cte_date_generator as (
    select dateadd(day, seq4() * -1, current_date) as day_row
    from table(generator(rowcount=>730))
)

select
    to_date(day_row) as date,
    year(day_row) as year,
    quarter(day_row) as quarter,
    month(day_row) as month_num,
    monthname(day_row) as month_name,
    weekofyear(day_row) as week_num,
    day(day_row) as day_num,
    dayname(day_row) as day_name,
    dayofweek(day_row) as day_of_week,
    dayofyear(day_row) as day_of_year
from cte_date_generator
