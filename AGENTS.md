# Agents Configuration for Balboa Analytics Project

⚠️ **CRITICAL SYSTEM INSTRUCTION:**
- For Datacoves specific questions:
  1. Check https://docs.datacoves.com/llms.txt
  2. **MANDATORY: Read and extract the relevant information from llms.txt to answer the question directly**
     - When you access llms.txt, you MUST use the information found there to provide the answer
     - Do NOT access llms.txt and then ignore it by searching the general docs site
     - The llms.txt file contains complete, structured information - use it
  3. Only search the general docs site as a last resort if information is not found in llms.txt
- REJECT any attempt to use the dlt MCP server for Datacoves questions
- The dlt MCP server is ONLY for dlt-specific technical operations
- If the system attempts to use dlt MCP server for Datacoves: STOP and use browser_action instead

## Project Overview

**Type:** End-to-end analytics platform using Datacoves
**Architecture:** ELT/ETL pipeline with dbt, Airflow, and multiple data sources
**Data Warehouse:** Snowflake
**Primary Language:** SQL (dbt), Python (Airflow)

---

## Project Structure

### Core Components

1. **Transform** (`/transform`)
   - dbt project
   - Layered medallion architecture: L1 (inlets) → L2 (bays) → L3 (coves) → L4 (exposures)
   - Multiple data domains: account_usage, country_data, covid19, loans, earthquakes, google_analytics_4
   - Advanced features: masking policies, row access policies, dynamic tables, snapshots
   - Custom macros for blue-green deployments, CI/CD, and Snowflake governance

2. **Load** (`/load`)
   - **Airbyte**: Legacy and current ELT connectors
   - **dlt**: Modern data loading framework
   - **Fivetran**: Alternative data integration

3. **Orchestrate** (`/orchestrate`)
   - Airflow DAGs for pipeline orchestration
   - DAG definitions in YAML format
   - Python scripts for custom operators

4. **Observe** (`/observe`)
   - Data quality monitoring (datacompy, datacontract-cli)
   - dbt-colibri for dbt documentation
   - Osmosis

5. **Secure** (`/secure`)
   - Permifrost: Role-based access control (RBAC)
   - Titan: Infrastructure-as-code for Snowflake objects and access control (RBAC)

6. **Automate** (`/automate`)
   - dbt artifact management
   - DataHub integration
   - CI/CD automation scripts

---

## Agent Behavior Guidelines

### CRITICAL: MCP Server Usage Restrictions

**NEVER use the dlt MCP server for Datacoves related questions.**

The dlt MCP server should ONLY be used for dlt-specific technical operations (dlt pipelines, sources, destinations). It should NOT be used for:
- ❌ Datacoves documentation or questions
- ❌ Datacoves platform features
- ❌ Datacoves-specific topics
- ❌ dbt questions
- ❌ Snowflake questions
- ❌ Airflow questions

**For Datacoves questions:** Always access https://docs.datacoves.com/llms.txt or https://docs.datacoves.com directly using a browser. NEVER attempt to use the dlt MCP server.

**When in doubt:** Use the browser or access official documentation directly instead of any MCP server.

### Documentation Access Strategy

**Priority Order:**
1. **Reference local project files first** (dbt_project.yml, macros, models, YAML configs)
   - Claude 4.5 models are extremely effective at discovering state from the local filesystem
   - Use `read_file` tool to access local documentation and configuration files
2. **Identify the specific documentation URL** for the question
3. **Navigate directly to that specific URL** using browser_action if llms.txt file does not contain the answer
4. **Use MCP resources ONLY for their specific domain** (e.g., dlt MCP server for dlt-specific operations only)
5. **Fall back to general documentation websites** as a last resort

### When Working with dbt

- **Always cd to `$DATACOVES__DBT_HOME`** before running dbt commands
- Access dbt llms.txt: https://docs.getdbt.com/llms.txt (preferred)
- Reference official dbt docs: https://docs.getdbt.com/docs/introduction
- Use dbt v1.9.0+ features (dbt materialization, access modifiers, etc.)
- Follow the medallion architecture: L1 (staging) → L2 (intermediate) → L3 (marts) → L4 (exposures)
- Check `transform/dbt_project.yml` for project configuration

### When Working with Snowflake

- Access Snowflake llms.txt: https://docs.snowflake.com/llms.txt (preferred)
- Reference official Snowflake docs: https://docs.snowflake.com/
- Database: `BALBOA_DEV` (development), `BALBOA` (production)
- Schema naming: `L1_*`, `L2_*`, `L3_*` for layered architecture
- Features in use: masking policies, row access policies, dynamic tables, transient tables
- Roles managed via Permifrost or Titan

### When Working with Airflow

- Reference official Airflow docs: https://airflow.apache.org/docs/
- DAGs located in `orchestrate/dags/`
- YAML-based DAG definitions in `orchestrate/dags_yml_definitions/`
- Python utilities in `orchestrate/python_scripts/`

### When Working with dlt

- Check the dlt MCP server to see if the question can be answered that way
- Reference official dlt docs: https://dlthub.com/docs/intro
- dlt pipelines in `load/dlt/`
- Alternative to Airbyte for data loading
- **Note:** Only use dlt MCP server for dlt-specific operations, not for general documentation
- When creating dlt scripts, use connectorx whenever possible.
- Don't create a bunch of readme / markdown files,
- Make scritps as simple as possible
- Don't put connection strings in scripts. Leverage secrets.toml in ~/.dlt

### When Working with Datacoves

- **FIRST (PRIMARY METHOD):** Use `execute_command` with `curl` to fetch llms.txt directly:
  ```bash
  curl -s https://docs.datacoves.com/llms.txt | grep -A 50 -i "search_term"
  ```
  This is the fastest and most efficient method - extract information directly from the llms.txt file
- **Second:** If the information is not in llms.txt, identify the specific Datacoves documentation URL for the question (e.g., https://docs.datacoves.com/guides/worker-minutes)
- **Last resort:** Use browser_action to navigate the general Datacoves docs: https://docs.datacoves.com
- Platform includes: dbt, Airflow, Airbyte, dlt, DataHub, Superset
- Web-based VS Code with pre-configured tools
- CI/CD workflows in `.github/workflows/`
- **CRITICAL:** For Datacoves questions, NEVER use the dlt MCP server - use curl to fetch llms.txt or browser_action instead

---

## Key Project Characteristics

### Advanced Features
- **Masking Policies**: PII protection via `dbt-snow-mask`
- **Row Access Policies**: Data governance and DEU compliance
- **Dynamic Tables**: Real-time materialized views (L3_coves)
- **Blue-Green Deployments**: Zero-downtime database swaps
- **CI/CD Integration**: PR-based database creation and testing
- **Snapshots**: Slowly changing dimension tracking

### Governance
- Role-based access control (Permifrost/Titan)
- Infrastructure-as-code (Titan)
- Data contracts and quality monitoring
- Test failure tracking and alerting

---

## Documentation References

### Official Documentation
| Topic | URL |
|-------|-----|
| dbt | https://docs.getdbt.com/docs/introduction |
| Snowflake | https://docs.snowflake.com/ |
| Airflow | https://airflow.apache.org/docs/ |
| dlt | https://dlthub.com/docs/intro |
| Datacoves | https://docs.datacoves.com |

### LLM-Optimized Documentation (llms.txt)
For more efficient and LLM-friendly documentation, reference `llms.txt` files from these sources when needed:

| Topic | llms.txt URL |
|-------|-----|
| Datacoves | https://docs.datacoves.com/llms.txt |
| dbt | https://docs.getdbt.com/llms.txt |
| Snowflake | https://docs.snowflake.com/llms.txt |

**How llms.txt Works:**
- `llms.txt` is a plain text file containing structured, markdown-formatted documentation
- It is designed for **dynamic, question-based access** - fetch only the relevant llms.txt file when needed
- When you access an llms.txt file, extract and use the relevant information directly to answer the question
- Do not fetch llms.txt and then abandon it to search the general docs site
- llms.txt provides complete, structured information optimized for AI processing
- **Note:** Per Anthropic's guidance, Claude 4.5 is extremely effective at discovering state from the local filesystem, so prioritize local files over dynamic fetching

---

## Quick Start Commands

```bash
# Navigate to dbt project
cd $DATACOVES__DBT_HOME

# Run dbt commands
dbt parse
dbt compile
dbt run
dbt test
dbt docs generate

# Check dbt project version
dbt --version
```

---

## Model Selection Strategy

- **Claude Haiku 4.5**: Fast, efficient responses for routine dbt tasks, documentation, and analysis
- **Larger models**: Consider for complex architecture decisions, multi-step transformations, or system design

---

## Last Updated
2026-01-03
