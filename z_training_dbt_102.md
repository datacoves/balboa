# dbt 102 training

## todo:
- Ensure prod run is complete and uploaded via dbt-artifacts before training delivered


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
```md
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
```md
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
```yml
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

- Show seeds file
balboa -> data -> state_codes
- Talk about persisting docs in db
`describe table balboa_dev.gomezn.state_codes`
Column-level comments are not supported on Snowflake views



## Demo - Project layout
- Describe project folder layout:
    - automate, load, schedule, transform, etc
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
    - models/bays/bay_covid/covid_cases
        Pivoted from Johns Hopkins data  
        Allows summarizing / comparing individual fields
        - Will be built at balboa_dev.(dev schema).covid_cases
    - models/coves/cove_covid/covid_cases_country, covid_cases_state, covid_cases_county
        Separated various geographic granularities, as county figures are duplicated as a sum in state, and states in country.
        These should never be queried together, so are separated to different models.

- Open dbt docs
    - Show searching for 'John Hopkins' in dbt docs and in vscode to easily find models by metadata
    - Write descriptions for searchability, not just usage
    - Describe the importance of discoverability - discovering by database location, or by folder



## Demo - Exposures
- Create file in the exposures folder named `covid_prediction.yml`

```yml
version: 2

exposures:
  - name: covid_infections_prediction
    # dashboard, notebook, analysis, ml, application
    type: ml
    maturity: medium
    url: https://datacoves.com/covid_predictions
    description: >
      Predicts the number of Covid-19 cases by country for a future weeks
    
    depends_on:
      - ref('covid_cases')
      - ref('location')
      - source('starschema_covid19', 'jhu_covid_19')
      
    owner:
      name: Santiago Pelufo
      email: santiago@datacoves.com
``` 
- build dbt docs
`dbt docs generate`
- Show the exposure
- change ml to `notebook`
- build dbt docs
`dbt docs generate`

## Demo - Packages
- Visit dbt hub `https://hub.getdbt.com`
- Talk about different types of packages, utils vs transformations
- visit the Snowplow package
- View the model snowplow/models/page_views/default/snowplow_web_page_context
https://github.com/dbt-labs/snowplow/blob/0.14.0/models/page_views/default/snowplow_web_page_context.sql
- Demonstrate dbt-external-tables
    - View the yml `models/sources/lineage/lineage_files.yml`
    - Run `dbt run-operation stage_external_sources --args "select: lineage" --vars "ext_full_refresh: true"`
    - In Snowflake, show `select * from raw.raw.lineage_processing;`

## Demo - Snapshots
- We've been advised that Johns Hopkins will stop maintaining the jhu_covid_19 dataset, and will only be maintaining the 'dashboard' dataset going forward.  
    This dataset shows the current information, rather than storing historical results.  
    We need to store historical information ourselves.
- Show `select * from starschema_covid19.public.jhu_dashboard_covid_19_global;`
- Discuss pros/cons of using an incremental table for this use case
    - incremental would not store previous values if data was updated to a more accurate value after the fact
- Show yml at models/sources/jhu_dashboard_covid_19_global.yml
    - Contains freshness tests:
        - Check data is fresh by running `dbt source freshness`
        - Adjust warn_after to 24 hours
- Show snapshot at snapshots/snp_jhu_dashboard_covid_19_global.sql
    - Discuss unique key, timestamp field
    - Move to a folder matching folder layout
    - Run `dbt snapshot` and show in Snowflake in (dev schema).snp_jhu_dashboard_covid_19_global
    - In Snowflake, discuss dbt_* fields
    - To use a snapshot, select `where dbt_valid_to is null`
- Discuss how snapshot can create the same output as jhu_covid_19, but only if it had been created at the start
    - can't recreate historical data from master data, so start snapshotting early
    - can't recreate missed versions of data, so snapshot frequently

## Demo - Materializations
- Show macros/helpers/materialized_view_materialization
    - Source: https://github.com/dbt-labs/dbt-labs-experimental-features
    - Other use cases:
        - lambda views
        - Snowflake streams + tasks (using change data capture to trigger runs between dbt runs)


## Demo - Selectors
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
    - Run `dbt run-operation empty_dev_schema --args '{dry_run: false}'` to empty dev schema (we'll look at macro later)
    - Use button `get prod metadata`, then run `dbt build --defer --select state:modified+`
        
- Buttons `get prod metadata` + `build all` - this should run almost everything needed while developing
    - Discuss small stories and continuous release to align with the above


## Demo - Modelling best practice
- Open coves/covid_cases_country, show difference with covid_cases_state
- Split logic to coves/cove_covid/int/int_covid_cases
- Point covid_cases_country, state, county to new intermediate
- General cleanup of DRYness in cove
- Move config to dbt_project from config blocks in cove_covid/agg models

## Demo - Macros
- Show usage of `generate_imports` macro in `models/coves/cove_covid/agg/agg_cases_by_month.sql`
    - Show code of `macros/helpers/generate_imports`
    - Add `generate_imports` macro to new int_covid_cases model created above, to replace CTEs
- Create new rank macro
    - Open `bay_covid/location.sql`,` bay_country/current_population.yml` to show repeated use of rank logic
    - Copy rank statement from either
    - Open macros/rank_desc.sql, show basic macro framework
    - Paste rank statement, and replace partition and order by with `{{ partition_fields }}` and `{{ datefield }}`
    - Discuss documentation of macros, importance of usage
        - Create `macros/rank_desc.yml` and document the rank_desc model (reference `macros/generate_imports.yml` for syntax)
        - Show documentation of new macro in docs
    - Reopen `bay_covid/location.sql` and replace inline rank with new macro
- Demonstrate debugging
    - Show logging in Macro `empty_dev_schema`
    - Create new macro `helpers/log_info.sql`

```
{% macro log_info(message) %}
    {%- do log(dbt_utils.pretty_log_format(message), true) -%}
{% endmacro %}
```

- Replace log rows in `empty_dev_schema` with `{{ log_info(message) }}
- Run `dbt run-operation empty_dev_schema` to demonstrate

## TODO: Set up performance analysis example
## TODO: Set up Testing example