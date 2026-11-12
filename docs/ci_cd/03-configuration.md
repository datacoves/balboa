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

The CI service user must also be able to use the `transformer_dbt` role. Stage 10 creates each PR database with it (the role is hardcoded in `transform/macros/tooling/create_database.sql`) and then transfers ownership to the role in `DATACOVES__MAIN__ROLE`. If your account uses a different role to create databases, change it in that macro.

The role in `DATACOVES__MAIN__ROLE` also needs `IMPORTED PRIVILEGES` on the `SNOWFLAKE` database, so the [PR database cleanup](31-pr-database-cleanup.md#2-sweep) in stage 30 can read `SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY`:

```sql
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE <ci_role>;
```

Without it, the sweep cannot list query activity, logs a warning and drops nothing. The deployment still succeeds.

Stage 30 also reads `DATACOVES__API_ENDPOINT`, `DATACOVES__API_TOKEN`, `DATACOVES__ACCOUNT_ID`, `DATACOVES__PROJECT_SLUG`, `DATACOVES__ENVIRONMENT_SLUG`, `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`. They are only used by demo steps (pushing artifacts to the Datacoves dbt API and uploading them to S3 for DataHub), so you don't need them if you remove those steps.

### CI images

Jobs run inside Datacoves CI images, so the tools match what developers use in their Datacoves environment:

| Image | Used by | Includes |
|---|---|---|
| `datacoves/ci-basic-dbt-snowflake:<version>` | dbt jobs in stages 10, 15, 20 and 30 | dbt, dbt-coves and pre-commit. pre-commit installs every hook in `.pre-commit-config.yaml` (dbt-checkpoint, SQLFluff, yamllint) in its own environment when the job runs, so hook versions come from that file, not from the image |
| `datacoves/ci-airflow-v3-dbt-snowflake:<version>` | Airflow job in stage 10 | Airflow 3, the Datacoves Airflow providers, and the `/usr/app/test_dags.py` script |

The Airflow major version is part of the image name (`ci-airflow-v3-*` for Airflow 3, `ci-airflow-v2-*` for Airflow 2), never the tag.

Pin the image tag to the Datacoves release your environment uses. This project uses:

```yaml
container: datacoves/ci-basic-dbt-snowflake:6.0
```

```yaml
container:
  image: datacoves/ci-airflow-v3-dbt-snowflake:6.0
```

The tags on [Docker Hub](https://hub.docker.com/u/datacoves) are:

| Tag | Example | Behavior |
|---|---|---|
| `MAJOR.MINOR` | `datacoves/ci-basic-dbt-snowflake:6.0` | **Recommended.** Moves to the newest published patch of that release line |
| `MAJOR.MINOR.PATCH` | `datacoves/ci-basic-dbt-snowflake:6.0.202611052110` | One published patch. `PATCH` is a UTC build timestamp (`YYYYMMDDHHMM`), so it differs between images of the same release: look it up on Docker Hub for each image |
| `MAJOR.MINOR.PATCH-<commit sha>` | `datacoves/ci-basic-dbt-snowflake:6.0.202611052110-2b4ed448` | One build; never moves. Every build gets this tag, including builds that were never published in a release |

Don't use a bare `MAJOR` tag such as `6`: it isn't one of the published release tags, and since several release lines (`6.0`, `6.1`, `6.2`) are published at the same time, it doesn't tell you which one you get.
