# dbt 102 training

## todo:
- Create an int in cove to join before countries, state, county
- Add an exposure to counties
- Add config blocks to each model in cove_covid/agg
- @noel - add an external table via S3
    - Create a Snowflake Stage connected to S3 https://docs.snowflake.com/en/sql-reference/sql/create-stage.html
    - Ensure queries work for the file we want (select * from @stage_name.file_name)
    - @chris - add the dbt-external-tables source
- Add custom materialization to slides (diagram of .sql in the middle, materialization options down the left)
- Create a macro (import a good example from dbt_utils)
    - create a custom log_info macro to wrap logging with info
    - Show logging
- Show freshness check

## Setup
- Ensure your dev schema is completely empty


## Demo 1 - Intro
- Describe project folder layout:
    - automate, schedule, transform, etc
    - transform/models/bays, coves, sources etc
- Walk through models:
    - models/sources/starschema_covid19/jhu_covid_19:  
        Johns Hopkins raw COVID data
        - Source at starschema_covid19.public.jhu_covid_19  
            Data source: starschema public Snowflake share - we just imported it to the database  
            https://app.snowflake.com/marketplace/listing/GZSNZ7F5UH
        - Base will be built at balboa_dev.(dev schema).jhu_covid_19
    - models/bays/bay_covid/location:
        Location data from jhu_covid_19  
        GPS coordinates change over time in the raw data as it gets more accurate  
        This model provides a single source of truth for geocoding - location "master data".  
        - Will be built at balboa_dev.(dev schema).location
    - models/bays/bay_covid/int_covid_cases
        Pivoted from Johns Hopkins data  
        Allows summarizing / comparing individual fields
        - Will be built at balboa_dev.(dev schema).int_covid_cases
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
- Open dbt docs
    - Show searching for 'John Hopkins' in dbt docs and in vscode to easily find models by metadata
    - Write descriptions for searchability, not just usage
    - Describe the importance of discoverability - discovering by database location, or by folder
- Open Metabase, show creation of a very basic graph using covid_cases_country
    - Create exposure using package
        - show code in vscode
        - show experience in docs

## Demo 2 - Selectors
- Edit `models/bays/bay_covid/location` to add the statement `where province_state ilike '%princess%'` to remove the cruise ships (setting up for state:modified)
- Run each, showing what is selected:
    - `dbt ls --select int_covid_cases`
    - Use button `run current` to run the above - describe the helpfulness of automations
    - `dbt ls --select coves.cove_covid.agg` lists all models in folder
    - `dbt ls --resource-type source` limits to a specific type
    - `dbt ls --select source:balboa.starschema_covid19.jhu_covid_19+`
    - `dbt ls --select source:balboa.starschema_covid19.jhu_covid_19+ --resource-type exposure` will show the exposure we created earlier
    - `dbt ls --select source:balboa.starschema_covid19.jhu_covid_19+,balboa.bays` shows only bays downstream
    - `dbt ls --select source:balboa.starschema_covid19.jhu_covid_19+1`
    - `dbt ls --select +int_covid_cases+ --exclude covid_cases_county`
    - `dbt ls --select @int_covid_cases` (includes upstream, downstream, and location, as parent of a child)
        - No need to use @ if we can defer
    - Use button `get prod metadata`, then run `dbt build --defer --select state:modified+`
        
- Buttons `get prod metadata` + `build all` - this should run almost everything needed while developing
    - Discuss small stories and continuous release to align with the above


## Demo 3 - Modelling best practice
- Open coves/covid_cases_country, show difference with covid_cases_state
- Split logic to coves/cove_covid/int/int_covid_cases
- Point covid_cases_country, state, county to new intermediate
- General cleanup of DRYness in cove
- Move config to dbt_project from config blocks in cove_covid/agg models