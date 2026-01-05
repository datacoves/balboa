# dbt-osmosis

## Set openai key from .env file
`set -a && source /config/workspace/observe/osmosis/.env && set +a`

## Run Refactor
Executes organize which syncs yaml files with database schema.
This also uses OpenAI to add descriptions to cols.

`uvx --with=dbt-snowflake~=1.8.0 --from 'dbt-osmosis[openai]' dbt-osmosis yaml refactor --synthesize`

Run without OpenAI

`uvx --with=dbt-snowflake~=1.8.0 dbt-osmosis yaml refactor`

You can pass `--fqn` with a name of a folder in `models/` to limit the refactor

`dbt-osmosis yaml refactor --fqn L3_coves`

Separate sub-folders with a `.`

`dbt-osmosis yaml refactor --fqn L3_coves.loan_analytics`
