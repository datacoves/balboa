# dbt-coves Commands Reference

This file contains commonly used dbt-coves commands for the Balboa project. Read this file to understand our dbt-coves workflow and command patterns.

## AI Assistant Instructions

**When the user asks to generate sources:**
1. Always ask which database to use with these options:
   - RAW (default from config)
   - Other database (let user specify)
2. Ask for the schema name(s) to process
3. Use `no-prompt` option in the command
4. Confirm the command before running it
5. When generating the source, give each, table, model, and column a description

**Example interaction:**
```
User: "Generate staging models for the table my_table"
Assistant: "Which database would you like to use for source generation?
- RAW (default)
- Other (please specify)

User: "Generate sources for the loans schema"
Assistant: "Which database would you like to use for source generation?
- RAW (default)
- Other (please specify)

User: "Generate source from the snowflake sample data database"
Assistant: "Which schema would you like to use for source generation?
Use the snowflake MCP server to list schemas
And which schema(s) should I process?"
```

## Project Configuration

Our dbt-coves is configured in `transform/.dbt_coves/config.yml` with:
- **Source Database**: `RAW`
- **Sources Destination**: `models/L1_inlets/{{schema}}/_{{schema}}.yml`
- **Models Destination**: `models/L1_inlets/{{schema}}/stg_{{relation}}.sql`
- **Model Props Destination**: `models/L1_inlets/{{schema}}/stg_{{relation}}.yml`
- **Update Strategy**: `ask` for properties, `update` for sources

## Common Commands

### Generate Sources
Generate source YAML files and staging models for a specific schema:

```bash
# Generate sources for a single schema (RAW database - default)
dbt-coves generate sources --database RAW --schemas PERSONAL_LOANS

# Generate sources for multiple schemas
dbt-coves generate sources --database RAW --schemas "PERSONAL_LOANS,COUNTRY_DATA"

# Generate sources with wildcard pattern
dbt-coves generate sources --database RAW --schemas "LOAN*"

# Generate sources from custom database
dbt-coves generate sources --database YOUR_DATABASE_NAME --schemas SCHEMA_NAME
```

**Available Databases:**
- `RAW` - Default raw data database (from config)
- Custom database name as needed

**What this does:**
1. Creates source YAML file: `models/L1_inlets/personal_loans/_personal_loans.yml`
2. Creates staging models: `models/L1_inlets/personal_loans/stg_*.sql`
3. Creates model property files: `models/L1_inlets/personal_loans/stg_*.yml`

### Generate Properties
Generate or update property files for existing models:

```bash
# Generate properties for all models in a directory
dbt-coves generate properties --models models/L1_inlets/loans/

# Generate properties for specific models
dbt-coves generate properties --models models/L1_inlets/loans/stg_personal_loans.sql
```

### Other Useful Commands

```bash
# Get help for any command
dbt-coves generate sources --help

# See all available commands
dbt-coves --help

# Generate metadata CSV for documentation
dbt-coves generate metadata --database RAW
```

## Existing Schemas in Our Project

Based on our dbt_project.yml, we have these L1_inlets schemas:
- `account_usage` → L1_ACCOUNT_USAGE
- `country_data` → L1_COUNTRY_DATA
- `country_geo` → L1_COUNTRY_GEO
- `google_analytics_4` → L1_GOOGLE_ANALYTICS_4
- `covid19_epidemiological_data` → L1_COVID19_EPIDEMIOLOGICAL_DATA
- `loans` → L1_LOANS
- `observe` → L1_OBSERVE
- `us_population` → L1_US_POPULATION
- `usgs__earthquake_data` → L1_USGS__EARTHQUAKE_DATA

## Templates

Our custom templates are located in `transform/.dbt_coves/templates/`:
- `source_props.yml` - Template for source YAML files
- `staging_model.sql` - Template for staging model SQL files
- `staging_model_props.yml` - Template for staging model property files

## Workflow Tips

1. **Always run from transform/ directory** (where dbt_project.yml is located)
2. **Check existing files first** - our update strategy is set to "ask" for properties
3. **Review generated files** - templates may need customization for specific use cases
4. **Use schema patterns** - our naming follows L1_INLETS → L2_BAYS → L3_COVES structure

## Troubleshooting

- If command fails, ensure you're in the `transform/` directory
- Check database connectivity in your dbt profiles
- Verify schema names exist in the RAW database or the database the user specifies
- Review `.dbt_coves/config.yml` for configuration issues
