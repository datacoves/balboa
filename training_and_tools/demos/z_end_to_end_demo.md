z_demo_script

* Find a dataset for Consumer Price Index
`https://www.bls.gov/cpi/data.htm`
`https://download.bls.gov/pub/time.series/ap/`

* Click on ap.data.2.Gasoline and search for:
  `APUN40074714`

* Get file url:
`https://download.bls.gov/pub/time.series/ap/ap.data.2.Gasoline`

* Set up Airbyte Source
`cpi_gasoline`

* Reader Options
`{ "sep" : "\t"}`

* Create the connection
  * talk about not scheduling
  * talk about not normalizing
* Start load

* Go to Snowflake
  * Switch role to loader airbyte
  * Show there is no cpi_gasoline schema

  * show cpi_gasoline schema
    * Talk about security

* Switch to Transform
  * Talk about the top level folders

* Create the dbt files
`dbt-coves generate sources --schema cpi_gasoline`

* Update SQL
  * Add TRIM around fields
`TRIM`
* Update `value` column
  * rename value to `price`
  * Change casting
`NUMBER(5,2)`

INTEGER
* add field
`DATE_FROM_PARTS( year, right(period,2), 1 ) as date`

* Add where condition to SQL
`where series_id = 'APUN40074714'`

* Show linting

* Show lowercaseing everything



Final code
```
with raw_source as (

    select
        *
    from {{ source('cpi_gasoline', '_airbyte_raw_cpi_gasoline') }}

),

final as (

    select
        trim(_airbyte_data:"       value")::number(5,2) as price,
        trim(_airbyte_data:"period"::varchar) as period,
        trim(_airbyte_data:"series_id        "::varchar) as series_id,
        trim(_airbyte_data:"year"::integer) as year,
        DATE_FROM_PARTS( year, right(period,2), 1 ) as date

    from raw_source

)

select * from final
where series_id = 'APUN40074714'
```

* Generate dbt docs with the button

* Show the schedule using `-tag`
* Show how we run Airflow in Airflow tab

* Create the query in SQL Lab
```
SELECT price,
       period,
       series_id,
       year, date
FROM gomezn._airbyte_raw_cpi_gasoline
```

* Click Explore
  * Change chart type to line
  * Add Price to metric