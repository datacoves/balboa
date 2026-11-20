# Database lifecycle

> Part of the [CI/CD pipelines](../ci_cd_pipelines.md) documentation.

Each stage creates, uses and drops its own Snowflake database, all derived from the base database name in `DATACOVES__MAIN__DATABASE`. Below, `<DATABASE>` is that base name.

![Database lifecycle: the PR database is created in stage 10, kept while the release is open and dropped after the stage 30 deploy, or in stage 15 if the PR is abandoned. The QA temp database is cloned and built in stage 20 and swapped into QA. The staging database is cloned and built in stage 30 and swapped with production.](../diagrams/cicd-database-lifecycle.png)

| Database | Created by | Used for | Dropped by |
|---|---|---|---|
| `<DATABASE>_PR_<pr number>` | Stage 10, for each feature PR into a release branch | Building and reviewing a single feature PR | Stage 15 if the PR is abandoned. Otherwise the [PR database cleanup](31-pr-database-cleanup.md) in stage 30, after the release that includes it is deployed, or earlier under the sweep's age limits |
| `<DATABASE>_QA_TEMP` | Stage 20, on the first run of the day, as a clone of production | Temporary database where the release is built | Stage 20: after the swap, this name holds the previous `_QA`, which is then dropped. If the build fails, it is left in place until the next run clones production into it again |
| `<DATABASE>_QA` | Stage 20, the first time it swaps `_QA_TEMP` into QA | Stable QA database with the release candidate. Later runs on the same day build directly in it | Replaced by a fresh clone on the first stage 20 run of a later day |
| `<DATABASE>_STAGING` (default name) | Stage 30 (`dbt-coves blue-green`) | Building the new production version | Stage 30: after the swap, this name holds the previous production version, which `dbt-coves blue-green` drops. If the deployment fails, it is left in place and blocks the next deployment (see [Blue/green deployment](30-deploy-to-production.md#bluegreen-deployment)) |
| `<DATABASE>` | Exists before the first deployment | Production. Stage 10 defers unchanged models to it, and stages 20 and 30 clone it. The demo's dynamic table refresh in stage 30 also writes to it directly | Never dropped under this name: each deployment swaps the new version in, and the previous one is dropped as `<DATABASE>_STAGING` |
