#! /bin/dash

# datahub delete --platform snowflake --hard
# datahub delete --platform dbt --hard

dbt source freshness -t prd
aws s3 cp /config/workspace/transform/target/sources.json s3://convexa-local/dbt_artifacts/sources.json
dbt build -s personal_loans -t prd
aws s3 cp /config/workspace/transform/target/manifest.json s3://convexa-local/dbt_artifacts/manifest.json
aws s3 cp /config/workspace/transform/target/run_results.json s3://convexa-local/dbt_artifacts/run_results.json
dbt docs generate -t prd
aws s3 cp /config/workspace/transform/target/catalog.json s3://convexa-local/dbt_artifacts/catalog.json

# dbt build -s personal_loans -t prd
# aws s3 cp /config/workspace/transform/target/run_results.json s3://convexa-local/dbt_artifacts/run_results.json
# aws s3 cp /config/workspace/transform/target/manifest.json s3://convexa-local/dbt_artifacts/manifest.json

# dbt docs generate -t prd
# aws s3 cp /config/workspace/transform/target/catalog.json s3://convexa-local/dbt_artifacts/catalog.json

# aws s3 cp s3://convexa-local/dbt_artifacts/run_results.json /config/workspace/transform/target/run_results.json
