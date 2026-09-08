# dbt Wizard

## Install dbt Wizard
curl -fsSL https://public.cdn.getdbt.com/dbt-wizard/install/install-wizard.sh | sh

## Configure OpenAI Subscription
1. In the terminal run `wizard providers configure`
2. Select `openai_subscription`
3. Open https://auth.openai.com/codex/device and enter device code after signing in
4. If step 3 fails, try it again
5. Select the OpenAI model, e.g. terra

## Add Datacoves Atlas MCP to dbt Wizard
1. the Env Var ATLAS_TOKEN
2. In the terminal run `wizard mcp add atlas --url https://dchealth.datacoves.ai/mcp --bearer-token-env-var ATLAS_TOKEN`
