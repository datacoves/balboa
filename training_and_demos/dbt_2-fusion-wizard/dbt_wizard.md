# dbt Wizard

## Install dbt Wizard
curl -fsSL https://public.cdn.getdbt.com/dbt-wizard/install/install-wizard.sh | sh

## Add Datacoves Atlas MCP to dbt Wizard
1. the Env Var ATLAS_TOKEN
2. In the terminal run `wizard mcp add atlas --url https://dchealth.datacoves.ai/mcp --bearer-token-env-var ATLAS_TOKEN`
