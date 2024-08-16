#! /bin/dash

dbt source freshness -t prd
dbt build -s personal_loans -t prd
dbt docs generate -t prd
aws s3 cp /config/workspace/transform/target/sources.json s3://convexa-local/dbt_artifacts/sources.json
aws s3 cp /config/workspace/transform/target/run_results.json s3://convexa-local/dbt_artifacts/run_results.json
aws s3 cp /config/workspace/transform/target/manifest.json s3://convexa-local/dbt_artifacts/manifest.json
aws s3 cp /config/workspace/transform/target/catalog.json s3://convexa-local/dbt_artifacts/catalog.json
