Setup:
Add button to settings.json:
                {
					"name": "📝 Create YML for current",
                    "cwd": "${fileDirname}",
                    "color": "white",
					"singleInstance": true,
					"command": "dbt run-operation generate_model_yaml --args '{'model_name': '${fileBasenameNoExtension}'}' | tail -n +2 | grep . > ${fileBasenameNoExtension}.yml" // This is executed in the terminal.
				},

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

Hotfix - a user has discovered empty values for `region_name`
    Create jira branch for 'Fill in empty region_name in country codes'
    Add tests to base_country_codes:
        `region_name`: unique
    dbt build --select _airbyte_raw_country_codes+
    Show errors, and point out continent field as a usable override
    Override `region_name` with `coalesce(region_name, case continent when 'AS' then 'Asia' when 'AN' then 'Antarctica' else 'Unknown' end) as region_name`

