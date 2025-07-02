# Snowflake Dynamic Tables with dbt Workshop

> **Hosted by [Datacoves](https://datacoves.com)** - Enterprise DataOps platform with managed dbt Core and Airflow for data transformation and orchestration.

This repository contains the materials and code examples from the **Snowflake Dynamic Tables with dbt** workshop held on **June 25, 2025**.

## Overview

This workshop demonstrates how to implement and work with Snowflake Dynamic Tables using dbt, showcasing the differences between standard tables and dynamic tables, along with advanced monitoring and alerting capabilities.

## Project Structure

### 📁 `/load`
Contains data loading scripts using dlt (data load tool):
- **`dlt/loans_data.py`** - Python script that loads personal loans data from a public S3 CSV file into Snowflake using dlt
- **`dlt/utils/datacoves_utils.py`** - Utility functions for dlt pipeline configuration

### 📁 `/transform`
Contains the dbt project for data transformation:
- **dbt Version**: Requires dbt >= 1.8.0
- **Project Name**: `balboa`
- **Profile**: Uses `default` profile for Snowflake connection

#### Key dbt Components:

**Models Structure:**
- **`L1_inlets/loans/`** - Staging models for loan data
  - `stg_personal_loans.sql` - Staging table for personal loans data, target lag = downstream
- **`L3_coves/loan_analytics/`** - Analytics models demonstrating dynamic vs standard tables
  - `loans_by_state__dynamic.sql` - Dynamic table implementation with 1-minute target lag
  - `loans_by_state__standard.sql` - Standard table implementation for comparison
- **`L1_inlets/observe/`** - Observability models
  - `stg_test_failures.sql` - View for tracking dbt test failures

**Seeds:**
- **`state_codes.csv`** - Reference data mapping state codes to state names

**Macros:**
- **`create_test_failure_view.sql`** - Creates a view that aggregates dbt test results for monitoring dynamic table data quality. This macro runs automatically after each dbt execution when `persist_tests` variable is set to true.
- **`generate_schema_name.sql`** - Custom schema naming logic that uses custom schema names in production but defaults to user schema in development.

### 📁 `/visualize`
Contains Streamlit application for real-time visualization:
- **`streamlit/loans-example/loans.py`** - Streamlit app that displays side-by-side comparison of standard vs dynamic tables with auto-refresh every 5 seconds
- **`streamlit/loans-example/database_connection.py`** - Snowflake connection utilities
- **`streamlit/loans-example/environment.yml`** - Conda environment specification

### 📁 Root Files
- **`script.sql`** - Basic workshop script demonstrating:
  - Enabling change tracking on source tables
  - Data manipulation and dbt builds
  - Monitoring dynamic table refresh behavior
- **`script2_advanced.sql`** - Advanced features including:
  - Creating streams on dynamic tables
  - Automated error capture using Snowflake tasks
  - Email alerting when data quality issues are detected

## Key Features Demonstrated

### 1. Dynamic Tables vs Standard Tables
- **Dynamic Table**: Automatically refreshes based on changes to upstream data with configurable target lag (1 minute in this example)
- **Standard Table**: Traditional materialized table that requires manual refresh
- **Warehouse**: Uses dedicated `wh_transforming_dynamic_tables` warehouse for dynamic table refreshes

### 2. Data Quality Monitoring
- Automated test failure tracking using custom macro
- Real-time monitoring of data quality issues
- Integration with Snowflake streams and tasks for proactive alerting

### 3. Advanced Snowflake Features
- **Change Tracking**: Enabled on source tables to support dynamic table refreshes
- **Streams**: Track changes in dynamic tables
- **Tasks**: Automated error capture and processing
- **Alerts**: Email notifications when data quality issues are detected

### 4. Real-time Visualization
- Streamlit application showing live comparison between table types
- Auto-refreshing charts and error monitoring
- Side-by-side visualization of data differences

## Getting Started

### Prerequisites
- Snowflake account with appropriate permissions
- dbt installed (version >= 1.8.0)
- uv installed for Python dependency management

### Setup Instructions

1. **Configure Snowflake Connection**
   - Set up your dbt profile for Snowflake connection
   - Ensure you have permissions to create dynamic tables

2. **Load Data**
   ```bash
   # The script is executable with uv run shebang
   ./load/dlt/loans_data.py
   ```

3. **Enable Change Tracking**
   ```sql
   ALTER TABLE RAW.LOANS.PERSONAL_LOANS SET CHANGE_TRACKING = true;
   ```

4. **Run dbt Build**
   ```bash
   cd transform
   dbt build --vars '{"persist_tests": "true", "tests_model": "test_failures"}'
   ```

5. **Launch Streamlit App**
   ```bash
   # Using the provided shell script
   ./visualize/streamlit/start_app.sh

   # Or directly with uvx
   uvx --with "schedule,snowflake-connector-python,snowflake-snowpark-python" streamlit run visualize/streamlit/loans-example/loans.py
   ```

## Workshop Scenarios

The workshop walks through several scenarios:

1. **Basic Setup**: Loading data and creating both standard and dynamic tables
2. **Data Changes**: Demonstrating how dynamic tables automatically refresh while standard tables remain static
3. **Error Detection**: Introducing data quality issues and showing automated detection
4. **Advanced Monitoring**: Setting up streams, tasks, and alerts for production-ready monitoring

## Configuration Notes

- **Target Lag**: Dynamic tables are configured with 1-minute target lag for demonstration purposes
- **Schema Strategy**: Uses custom schema names in production, user schemas in development
- **Test Storage**: Test failures are stored as views in `DBT_TEST__AUDIT` schema
- **Time Zone**: Configured for America/Los_Angeles timezone

## Workshop Takeaways

- Dynamic tables provide automatic refresh capabilities based on upstream changes
- Proper monitoring and alerting are crucial for production dynamic table implementations
- dbt macros can be used to create sophisticated monitoring solutions
- Snowflake's native features (streams, tasks, alerts) integrate well with dbt workflows

---

*This repository serves as a reference implementation for the concepts covered in the Snowflake Dynamic Tables with dbt workshop. Feel free to explore the code and adapt it for your own use cases.*
