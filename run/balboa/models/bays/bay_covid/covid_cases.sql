
  create or replace  view staging_BALBOA.bay_covid.covid_cases 
  
    
    
(
  
    
      LOCATION_ID
    
    , 
  
    
      DATE
    
    , 
  
    
      CONFIRMED
    
    , 
  
    
      DEATHS
    
    , 
  
    
      ACTIVE
    
    , 
  
    
      RECOVERED
    
    
  
)

  copy grants as (
    with raw_cases as (
    select
        country_region,
        province_state,
        county,
        cases,
        date,
        case_type
    from starschema_covid19.public.JHU_COVID_19
),

create_location_id as (
    select
        md5(cast(coalesce(cast(country_region as 
    varchar
), '') || '-' || coalesce(cast(province_state as 
    varchar
), '') || '-' || coalesce(cast(county as 
    varchar
), '') as 
    varchar
)) as location_id,
        cases,
        date,
        case_type
    from raw_cases
),

pivoted_model as (
    select
        location_id,
        date,
        sum("'Confirmed'") as confirmed,
        sum("'Deaths'") as deaths,
        sum("'Active'") as active,
        sum("'Recovered'") as recovered
    from create_location_id
    pivot (sum(cases) for case_type in( 'Confirmed', 'Deaths', 'Active', 'Recovered' )) as case_pivot
    group by location_id, date
)

select *
from pivoted_model
  );
