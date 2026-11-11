# Configuration

> Part of the [CI/CD pipelines](../ci_cd_pipelines.md) documentation.

Variables, secrets and CI images that every stage needs.

## Required configuration

All stages connect to Snowflake through `automate/dbt/profiles.yml`, which reads its values from environment variables. Add them under **Settings › Secrets and variables › Actions** as repository variables and secrets. The workflows read them with `${{ vars.NAME }}` and `${{ secrets.NAME }}`.

| Name | Type | Description |
|---|---|---|
| `DATACOVES__MAIN__ACCOUNT` | Variable | Snowflake account |
| `DATACOVES__MAIN__DATABASE` | Variable | Base (production) database name, without suffixes |
| `DATACOVES__MAIN__SCHEMA` | Variable | Default schema |
| `DATACOVES__MAIN__ROLE` | Variable | Role used by the CI service user |
| `DATACOVES__MAIN__WAREHOUSE` | Variable | Warehouse |
| `DATACOVES__MAIN__USER` | Variable | CI service user |
| `DATACOVES__MAIN__PRIVATE_KEY` | **Secret** | Private key of the service user, in P8 format (see [Snowflake key-pair authentication](https://docs.datacoves.com/docs/how-tos/snowflake/snowflake-key-based-auth)) |
| `DATACOVES__DROP_DB_ON_FAIL` | Variable | When `true`, stage 10 drops the PR database if grants cannot be applied |

The production manifest used for Slim CI is read from, and written to, a Snowflake internal stage by the `get_last_artifacts` and `upload_artifacts` macros (default `RAW.DBT_ARTIFACTS.ARTIFACTS`).

### CI images

Jobs run inside Datacoves CI images, so the tools match what developers use in their Datacoves environment:

| Image | Used by | Includes |
|---|---|---|
| `datacoves/ci-basic-dbt-snowflake:<version>` | dbt jobs in stages 10, 15, 20 and 30 | dbt, dbt-coves, pre-commit and SQLFluff. The other pre-commit hooks, such as dbt-checkpoint and yamllint, are installed by pre-commit from `.pre-commit-config.yaml` when the job runs |
| `datacoves/ci-airflow-v3-dbt-snowflake:<version>` | Airflow job in stage 10 | Airflow, the Datacoves Airflow providers, and the `/usr/app/test_dags.py` script |

Pin the image tag to the Datacoves release your environment uses. See [Datacoves versioning](https://docs.datacoves.com/docs/reference/datacoves/versioning) for the available tag formats.
