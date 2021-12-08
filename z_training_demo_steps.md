# Setup:
### To do:
Add a feature-1 branch to merge to release branch for reversion

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
Add $DBT_HOME to /config/.bashrc:
```
export DBT_HOME="/config/workspace/transform"
```

### Reset Environment:
- Ensure `current_population.sql` has no aliases
- Remove all models and yml not related to population in models/country_analysis
- Delete all local and github branches related to dataops-training
- Create release/dataops-training branch from main
- Merge feature-1 to release (for later reversion)
- Delete Airbyte country codes source & connection


# Demo 1:

Start Jira story for ingesting data

- Set up Airbyte source as file:
https://raw.githubusercontent.com/datasets/country-codes/master/data/country-codes.csv
- Set up connection:
    - Sync frequency manual
    - country_codes sync mode: full refresh | overwrite
    - raw data - no normalization
- show table in balboa.raw

# Demo 2:

Start Jira story for creating model

Create new git branch for jira story git checkout -b feature/country_codes/DD-3

- dbt-coves generate sources --database raw
  - Select _airbyte_raw_country_codes
  - Yes to flatten

Show created table and flattened version in sqltools

Change `M49` to integer in flattening model

Add metadata & tests to _airbyte_raw_country_codes:
- description on source & model: "Raw country code data from GitHub datasets repository"
- Tests:
```
    # Model:
    tests:
        - dbt_expectations.expect_table_row_count_to_be_between:
            min_value: 200
            max_value: 400
    # cldr_display_name:
    tests:
        - not_null
        - unique
    # developed___developing_countries
    tests:
        - accepted_values:
            values:
            - 'Developed'
            - 'Developing'
```
- `dbt build --select _airbyte_raw_country_codes`
- Show errors in snowflake by copying failure sql statement and running in sqltools
    - Describe error - CLDR display name is the primary key and is null

On error:
- Set not_null test on `cldr_display_name` to warning for our flattening model (we'll add a base model to error):
```
- not_null:
    severity: warning
```
- Create base model `base_country_codes` in models/bay_country to deal with null values:
    - Get the field names for the .sql:
        - `select {{ dbt_utils.star(ref('_airbyte_raw_country_codes')) }} from balboa.raw._airbyte_raw_country_codes`
        - Build current and copy fields from target/run folder to add to model
    - Add new column `coalesce(cldr_display_name, official_name_en)`, alias to `display_name`
    - Add .yml for new base model with tests
        - Click 'Create model YML'
        - Add description "Cleaned up country codes"
        - Add test `display_name`: not_null, unique
    - dbt build --select _airbyte_raw_country_codes+
    - Stage changes and run checks

In `current_population.sql`:
- Alias `value` to `population`

Create Bay model `countries`

```
select
    countries.display_name,
    countries.region_name,
    countries.iso4217_currency_name as currency,
    current_population.population
from {{ ref('base_country_codes') }} as countries
left join {{ ref('current_population') }} as current_population
    on
        current_population.country_code = countries.iso3166_1_alpha_3

  ```
- Create YML for current, and add description "Compiled Countries information"; and appropriate column descriptions
- Stage, run checks, commit and push
- Create Pull Request to release branch

**Go back to slides**


# Demo 3:

Hotfix - a user is cleaning up `current_population.sql`
- Create hotfix branch off main
- Rename `value` to `country_population`
- Rename `year` to `population_year`
- Make changes in .yml also, with descriptions
- Commit and push
- Create pull request, wait for CI, and merge

Continue with original release:
- Merge pull request to release branch
- Create pull request to main from release branch
- In GitHub interface, 'Resolve Conflicts'
  - Edit to `value as population, year as population_year`
  - Mark as resolved
  - Commit merge


