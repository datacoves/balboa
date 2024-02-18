

### Reset Environment:
- Ensure `models/bays/bay_country/current_population.sql` has no aliases
- Remove all models and yml not related to population in models/country_analysis
- Delete all local and github branches related to dataops-training
- Create release/dataops-training branch from main
- Create feature/training-feature-to-revert from main
    - Add Nevada to list
- Merge feature/training-feature-to-revert to release/dataops-training branch (for later reversion)
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

Create new git branch for jira story `git checkout -b feature/country_codes/DD-3`

- dbt-coves generate sources
  - Select _airbyte_raw_country_codes
  - Yes to flatten

Show created table and flattened version in sqltools

Change `M49` to integer in flattening model

Add metadata & tests to _airbyte_raw_country_codes:

- Tests:
```yaml
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
        gitests:
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

```yaml
        - not_null:
            severity: warn
```
- Create base model `base_country_codes` in models/bay_country to deal with null values:
    - Copy the following into base_country_codes.sql:

````sql
    SELECT
        "CLDR_DISPLAY_NAME",
        "CAPITAL",
        "CONTINENT",
        "DS",
        "DEVELOPED___DEVELOPING_COUNTRIES",
        "DIAL",
        "EDGAR",
        "FIFA",
        "FIPS",
        "GAUL",
        "GEONAME_ID",
        "GLOBAL_CODE",
        "GLOBAL_NAME",
        "IOC",
        "ISO3166_1_ALPHA_2",
        "ISO3166_1_ALPHA_3",
        "ISO3166_1_NUMERIC",
        "ISO4217_CURRENCY_ALPHABETIC_CODE",
        "ISO4217_CURRENCY_COUNTRY_NAME",
        "ISO4217_CURRENCY_MINOR_UNIT",
        "ISO4217_CURRENCY_NAME",
        "ISO4217_CURRENCY_NUMERIC_CODE",
        "ITU",
        "INTERMEDIATE_REGION_CODE",
        "INTERMEDIATE_REGION_NAME",
        "LAND_LOCKED_DEVELOPING_COUNTRIES__LLDC_",
        "LANGUAGES",
        "LEAST_DEVELOPED_COUNTRIES__LDC_",
        "M49",
        "MARC",
        "REGION_CODE",
        "REGION_NAME",
        "SMALL_ISLAND_DEVELOPING_STATES__SIDS_",
        "SUB_REGION_CODE",
        "SUB_REGION_NAME",
        "TLD",
        "UNTERM_ARABIC_FORMAL",
        "UNTERM_ARABIC_SHORT",
        "UNTERM_CHINESE_FORMAL",
        "UNTERM_CHINESE_SHORT",
        "UNTERM_ENGLISH_FORMAL",
        "UNTERM_ENGLISH_SHORT",
        "UNTERM_FRENCH_FORMAL",
        "UNTERM_FRENCH_SHORT",
        "UNTERM_RUSSIAN_FORMAL",
        "UNTERM_RUSSIAN_SHORT",
        "UNTERM_SPANISH_FORMAL",
        "UNTERM_SPANISH_SHORT",
        "WMO",
        "IS_INDEPENDENT",
        "OFFICIAL_NAME_AR",
        "OFFICIAL_NAME_CN",
        "OFFICIAL_NAME_EN",
        "OFFICIAL_NAME_ES",
        "OFFICIAL_NAME_FR",
        "OFFICIAL_NAME_RU",
        "_AIRBYTE_AB_ID",
        "_AIRBYTE_EMITTED_AT"
    from {{ ref('_airbyte_raw_country_codes') }}
````


- Clean up fields - (option+shift+i for multicursor), remove quotes & change to lowercase.
- Add new column
    `coalesce(cldr_display_name, official_name_en) as display_name,`
- build model
- Add .yml for new base model with tests
    - Click 'Create model YML'
    - Add description "Cleaned up country codes"
    - Add test `display_name`:
```yaml
        tests:
            - not_null
            - unique
```
- Click Build changes
    `dbt build --select _airbyte_raw_country_codes+`
- Stage changes
- run checks
- Models will fail bec they dont have descriptions
- Add descriptions to models:
- source model:
`description: Raw country code data from GitHub datasets repository`
- bay model:
`description: Cleaned up country codes`

In `current_population.sql`:
- Alias `value` to `population` (will need to be changed in yml also)

Create Bay model `countries.sql`

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
- Create hotfix branch for jira story `git checkout -b hotfix/current_population/DD-4`
- Rename columns
```sql
    value as country_population,
    year as population_year
```
- Make changes in .yml also
```
      - name: country_code
        description: "3 Letter Country Code"
      - name: country_name
        description: "Name of the country"
      - name: country_population
        description: "Total population for the country"
      - name: population_year
        description: "Year population was collected"
```
- Commit and push
- Create pull request, wait for CI, and merge

Continue with original feature:
`git checkout feature/country_codes/DD-3`
- Pull main to feature branch
`git pull origin main`
- Fix the merge conflicts
- Commit and push changes
- Merge PR
