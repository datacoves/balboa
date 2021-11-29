# Setup:
### Add button to settings.json:
```
{
    "name": "📝 Create YML for current",
    "cwd": "${fileDirname}",
    "color": "white",
    "singleInstance": true,
    "command": "dbt run-operation generate_model_yaml --args '{'model_name': '${fileBasenameNoExtension}'}' | tail -n +2 | grep . > ${fileBasenameNoExtension}.yml" // This is executed in the terminal.
},
```

### Reset Environment:
- Ensure `current_population.sql` has no aliases
- Remove all models and yml not related to population in models/country_analysis
- Delete all local and github branches related to dataops-training
- Create release/dataops-training branch from master

Population and current_population models should be in branch and db




Demo:

Create a Jira task for 'Add Countries dataset'

Set up Airbyte source:
    https://raw.githubusercontent.com/datasets/country-codes/master/data/country-codes.csv
    Set up connection:
    Sync frequency manual
    country_codes sync mode: full refresh | overwrite
    raw data - no normalization

dbt-coves generate sources
    Select _airbyte_raw_country_codes
    Yes to flatten

Add tests to _airbyte_raw_country_codes on:
    model:
        - dbt_expectations.expect_table_row_count_to_be_between:
            min_value: 200
            max_value: 400
    `cldr_display_name`: 
        - not_null
        - unique
    `developed___developing_countries`: 
        - accepted_values:
            values:
              - 'Developed'
              - 'Developing'
    dbt build --select _airbyte_raw_country_codes+
    Show errors in sqltools

On error:
    Remove null test from `cldr_display_name`
    Create base model `base_country_codes` to deal with null values:
        Get the field names for the .sql:
            select {{ dbt_utils.star(ref('_airbyte_raw_country_codes')) }} from {{ ref('_airbyte_raw_country_codes') }}
            `dbt compile` and copy fields from target folder to add to model
        Override `cldr_display_name` with `coalesce(cldr_display_name, official_name_en)`, alias to `display_name`
        Add .yml for new base model with tests
            Click 'Create model YML'
            Add test `display_name`: not_null, unique
        dbt build --select _airbyte_raw_country_codes+

In `current_population.sql`:
- Alias `value` to `population`

Create Bay model `countries`

```
select
    countries.display_name,
    countries.iso4217_currency_name as currency,
    current_population.population
  from {{ ref('base_country_codes') }} as countries
  left join {{ ref('current_population') }} as current_population
  on current_population.country_code = countries.iso3166_1_alpha_3
  ```

- Create YML for current, and add description "Compiled Countries information"; and appropriate column descriptions
- Stage, run checks, commit and push
- Show checks running on Push (wait for this to complete before starting PR)
- Create Pull Request to release branch

**Go back to slides**


# Demo 2:

Hotfix - a user is cleaning up `current_population.sql`
- Create hotfix branch off master
- Rename `value` to `country_population`
- Rename `year` to `population_year`
- Make changes in .yml also, with descriptions
- Commit and push
- Wait for CI Push, create pull request, and merge

Continue with original release:
- Merge pull request to release branch
- Create pull request to master from release branch
- In GitHub interface, 'Resolve Conflicts'
  - Edit to `value as population, year as population_year`
  - Mark as resolved
  - Commit merge

