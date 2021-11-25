Set up Airbyte source:
    https://raw.githubusercontent.com/datasets/country-codes/master/data/country-codes.csv
    Set up connection:
    Sync frequency manual
    country_codes sync mode: full refresh | overwrite
    raw data - no normalization

dbt-coves generate sources
    Select _airbyte_raw_country_codes
    Yes to flatten

Add not_null and unique tests to _airbyte_raw_country_codes

