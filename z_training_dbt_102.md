# dbt 102 training

## Setup
- Ensure your dev schema is completely empty


## Demo 1

- Walk through models:
    - models/sources/starschema_covid19/jhu_covid_19:  
        Johns Hopkins raw COVID data
        - Source at starschema_covid19.public.jhu_covid_19  
            Data source: starschema public Snowflake share - we just imported it to the database
        - Base will be built at balboa_dev.<dev schema>.jhu_covid_19
    - models/bays/bay_covid/location:
        Location data from jhu_covid_19
        GPS coordinates change over time in the raw data as it gets more accurate
        This model provides a single source of truth for geocoding - location "master data".
        - Will be built at balboa_dev.<dev schema>.location
    - models/bays/bay_covid/int_covid_cases
        Pivoted from Johns Hopkins data
        Allows summarizing / comparing individual fields
        - Will be built at balboa_dev.<dev schema>.int_covid_cases
    - models/coves/cove_covid/covid_cases_country, covid_cases_state, covid_cases_county
        Separated various geographic granularities, as county figures are duplicated as a sum in state, and states in country.
        These should never be queried together, so are separated to different models.

- Open Snowsight  
    Run `select * from starschema_covid19.public.jhu_dashboard_covid_19_global;`
    - Use profiling column on right, to tour the data
    - Click on the first date, show Diamond Princess etc (cruise ships in early covid outbreak) to show exceptions
    - Describe how snapshotting this table since day 1 would produce same results as jhu_covid_19
    - Describe importance of profiling tool in creating sources
        - Will need to remove old dates from dashboard to get a current picture of the world
- Discuss folder structure and database structure in dbt docs
    - Show searching for 'John Hopkins' in dbt docs and in vscode to easily find models by metadata
    - Write descriptions for searchability, not just usage

## Demo 2
- Edit `models/bays/bay_covid/location` to add the statement `where province_state ilike '%princess%'` to remove the cruise ships (setting up for state:modified)
- Run each, showing what is selected:
    - `dbt ls --select int_covid_cases`
    - Use button `run current` to run the above - describe the helpfulness of automations
    - `dbt ls --select coves.cove_covid.agg` lists all models in folder
    - `dbt ls --select +int_covid_cases+`
    - `dbt ls --select +int_covid_cases+ --exclude covid_cases_county`
    - `dbt ls --select @int_covid_cases` (includes upstream, downstream, and location, as parent of a child)
    - Use button `get prod metadata`, then run `dbt build --defer --select state:modified+`
        - No need to use @ if we can defer 
- Buttons `get prod metadata` + `build all` - this should run almost everything needed while developing
    - Discuss small stories and continuous release to align with the above
