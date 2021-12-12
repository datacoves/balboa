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

- YML topics
    - Show how to do long descriptions

    - add Docs path to dbt_project.yml
        `docs-paths: ["docs"]`


## Setup
- Ensure your dev schema is completely empty
Run `dbt run-operation reset_for_dbt_102`


## Demo 0 - Docs Overview
- Show dbt docs to connect what they just saw
- Open lineage graph, show components, show filtering 
add `tag:daily_afternoon` to --select
add `+` at the end
change to `base_cases`
add `+` at the start
add `+` at the end
add `+` at both ends
add `1` at the end
- Show Project vs Database view
    - Sources
    - Exposures
    - Projects
        every package that gets imported into dbt will show as a project

- Click on exposures and show how they are linked to viz

- Show folder overview pages
- Add landing page for dbt_artifacts by updating overview.md
```
{% docs __dbt_artifacts__ %}
# DBT Artifacts
This package enables capture of dbt artifacts and analysis such as finding the critical path of the dbt dag.

More information can be found on their <a href="https://github.com/tailsdotcom/dbt_artifacts/tree/0.5.0/" target="_blank">project page</a>
{% enddocs %}
```
- build dbt docs
`dbt docs generate`
- Every project folder can get an overview, even Exposures, but not sources

- Add Desciption to starschema_covide19 source
    - Create file `jhu_covid_19.md`
```
{% docs jhu_covid_19 %}
# Starschema Covid 19
This data comes from the Snowflake Data Marketplace

This data can be used as a single source of truth regarding the coronavirus outbreak, assess contingency plans and make informed, data-driven decisions in view of the global health emergency. In addition, various other data sources are included that bear on the handling of the pandemic, such as healthcare resource availability, demographics and testing data. Vaccination information, as well as data on the load on the healthcare system, can assist businesses and individuals in monitoring the progress of the pandemic.

More information can be found <a href="https://app.snowflake.com/marketplace/listing/GZSNZ7F5UH" target="_blank">here</a>
{% enddocs %}
```

- add description to jhu_covid_19.yml
`description: '{{ doc("jhu_covid_19") }}' `  

- add other attributes to the jhu_covid_19.yml
`loader: Snowflake Data Marketplace`

add a long description to the source to show multiline 
```
        description: >
          Lorem ipsum dolor sit amet, consectetur adipiscing elit, 
          sed do eiusmod tempor incididunt ut labore et dolore magna 
          aliqua. Ut enim ad minim veniam, quis nostrud exercitation 
          ullamco laboris nisi ut aliquip ex ea commodo consequat. 
          Duis aute irure dolor in reprehenderit in voluptate velit 
          esse cillum dolore eu fugiat nulla pariatur.

```
add ** in description to show bold (markdown)

- build dbt docs
`dbt docs generate`

- Create folder `assets`
- edit dbt_project.yml and add assets path
`asset-paths: ["assets"]`
- drag image from desktop to the assets folder
- add covid image to `jhu_covid_19.md`
`![Corona image](assets/covid_19.jpeg)`

- build dbt docs
`dbt docs generate`

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