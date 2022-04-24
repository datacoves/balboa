

-- setup




with a as (

    select * from BALBOA.bay_covid.covid_cases

),

b as (

    select * from BALBOA.seeds.covid_cases_expected_values

),

b_minus_a as (

    select "LOCATION_ID", "DATE", "CONFIRMED", "DEATHS", "ACTIVE", "RECOVERED" from b
    

    except


    select "LOCATION_ID", "DATE", "CONFIRMED", "DEATHS", "ACTIVE", "RECOVERED" from a

)

select * from b_minus_a

