# Stage 30: Deploy to production

> Part of the [CI/CD pipelines](../ci_cd_pipelines.md) documentation.

`30_deploy_changes_to_production.yml` triggers on `push` to `main` when files change in `.github/workflows/**`, `automate/**` or `transform/**`.

It can also be run manually, for example to redeploy after a failure. A manual run reads the full-refresh marker from the latest commit on `main`, which is usually the version commit of the previous deployment, so it never runs a full refresh. The targeted PR database cleanup finds no PR for that commit and is skipped; only the sweep runs.

> [!WARNING]
> The workflow has no `concurrency` group, so two merges into `main` in quick succession start two deployments in parallel. They compete for the same staging database, and the second one usually fails with `Green database ... already exists`. They can also compute the same version number. Wait for a deployment to finish before merging the next PR into `main`.

The job runs in the `datacoves/ci-basic-dbt-snowflake` image with `DATACOVES__MAIN__DATABASE` set to the production database.

| # | Step | What it does |
|---|---|---|
| 1 | Checkout, set secure directory, `dbt deps` | Checks out `main` with full history and installs packages |
| 2 | Get prod manifest | `get_artifacts.sh` downloads the manifest of the **previous** deployment, used to deploy only what changed |
| 3 | Check for full-refresh marker | Looks for `[deploy:full-refresh]` in the latest commit message (added by [stage 20](20-release-checks.md#why-a-deploy-marker-in-the-pr-title)). If found, the deployment uses `--full-refresh` |
| 4 | Set deployment version and tag | `automate/dbt/set_deployment_version.sh` (see [Versioning](#versioning)) |
| 5 | Blue/green deployment | `dbt-coves blue-green --defer` when a production manifest exists, or `dbt-coves blue-green` for a full build when it does not (see [Blue/green deployment](#bluegreen-deployment)) |
| 6 | Restore grants | `grant_db_usage` restores the select grants on tables, views and dynamic tables that are not carried across the database swap |
| 7 | Drop orphaned relations | `drop_orphaned_relations` drops tables and views that still exist in production but are no longer defined in dbt. It only scans schemas that still contain at least one dbt model, seed or snapshot |
| 8 | Drop orphaned schemas | `orphaned_schemas.py` compares the previous production manifest with the new one to find schemas whose models were all deleted, and `drop_orphaned_schemas` drops them. Only schemas that dbt created are considered, and schemas that contain objects dbt did not build are kept |
| 9 | Generate and publish docs | `dbt docs generate`, then publishes `transform/target` to the `dbt-docs` branch, which Datacoves serves as the project's dbt docs |
| 10 | Upload dbt artifacts | `upload_artifacts` uploads `manifest.json`, `run_results.json` and `catalog.json` to the Snowflake stage (and a timestamped copy under `archive/`), where stages 10, 20 and the next deployment read them |
| 11 | Push version changes | Pushes the version commit and tag created in step 4 to `main` |
| 12 | PR database cleanup | Drops the feature PR databases that are no longer needed (see [PR database cleanup](31-pr-database-cleanup.md)) |

The workflow also includes steps that belong to this demo, such as refreshing dynamic tables before the blue/green run, deploying a Streamlit app, uploading artifacts to S3 for DataHub, and pushing artifacts to the Datacoves dbt API. The dynamic table refresh runs `dbt build` directly against the production database, outside the blue/green deployment: it rebuilds the dynamic tables, `stg_test_failures` and the seeds in production, with `--full-refresh` when the marker is present, even if the blue/green run fails afterwards.

### If a step fails after the swap

Production already runs the new version, but the steps after the failed one do not run:

- If it fails before step 10, the artifacts are not uploaded. Stages 10 and 20 and the next deployment keep using the previous manifest, so Slim CI compares against a state that is no longer production's.
- The version commit and tag (step 11) are not pushed.
- The PR database cleanup (step 12) does not run. The sweep catches up on the next deployment.

Fix the cause and run the workflow manually to redeploy and upload the artifacts again.

## Blue/green deployment

`dbt-coves blue-green` builds the new version of production next to the current one, and swaps them only when everything succeeded:

1. Creates a staging database (by default `<DATABASE>_STAGING`) and zero-copy clones every production schema into it, with its tables and grants, so the models that did not change are already there. It also copies the database's future and inherited grants, so they apply to the objects dbt creates.
2. Runs `dbt build` against the staging database. With `--defer`, only modified models and their descendants are built.
3. Copies the database-level grants from production to staging, then the ownership and managed access settings.
4. Swaps staging and production with `ALTER DATABASE ... SWAP WITH ...`.
5. Drops the previous production database, which now has the staging name.

If any model or test fails, the swap never happens and the blue/green run leaves production untouched (the demo's dynamic table refresh, described above, runs before it and is not covered). The failed staging database is left in place for debugging, unless `--drop-staging-db-on-failure` is used.

> [!WARNING]
> While a failed staging database exists, the next `dbt-coves blue-green` run stops with an error because the staging database already exists. Drop it before re-running the deployment, or add `--drop-staging-db-at-start` or `--drop-staging-db-on-failure` to the command.

In this repository the options are set under `blue_green:` in `transform/.dbt_coves/config.yml`; `drop_staging_db_at_start`, `drop_staging_db_on_failure` and `keep_staging_db_on_success` are all `false`.

See the [`dbt-coves blue-green` reference](https://github.com/datacoves/dbt-coves/tree/main/docs/commands/blue-green) for all options, such as `--staging-database` to use a fixed staging database name and `--keep-staging-db-on-success` to keep the previous production version as a rollback copy.

## Versioning

Every deployment gets a version in the format `YYYYMMDD.N.0`, where the date is in UTC and `N` is the deployment number for that day. For example, the second deployment on August 1, 2026 is `20260801.2.0`.

`set_deployment_version.sh`:

1. Fetches existing tags and finds the highest `N` already used today.
2. Writes the new version to the `version:` key of `dbt_project.yml`. The trailing `.0` keeps it a valid three-part semantic version, as dbt requires.
3. Commits the change and creates a git tag with the same name. The commit message and the tag message include `[skip ci]` so the push does not trigger stage 30 again.

The version is bumped **before** any step writes dbt artifacts, because dbt stores a hash of `dbt_project.yml` in `partial_parse.msgpack`. Artifacts built before the bump could not be reused against the version that ends up on `main`.
