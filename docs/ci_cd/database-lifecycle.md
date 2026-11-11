# Database lifecycle

> Part of the [CI/CD pipelines](../ci_cd_pipelines.md) documentation.

Which Snowflake database each stage creates, uses and drops.

Every stage works against a different Snowflake database, all derived from the base database name in `DATACOVES__MAIN__DATABASE`. Below, `<DATABASE>` is that base name.

![Database lifecycle: the PR database is created in stage 10, kept while the release is open and dropped after the stage 30 deploy, or in stage 15 if the PR is abandoned. The QA temp database is cloned and built in stage 20 and swapped into QA. The staging database is cloned and built in stage 30 and swapped with production.](../diagrams/cicd-database-lifecycle.png)

| Database | Created by | Used for | Dropped by |
|---|---|---|---|
| `<DATABASE>_PR_<pr number>` | Stage 10 | Building and reviewing a single feature PR | Stage 15 if the PR is abandoned; stage 30 after the release that includes it is deployed |
| `<DATABASE>_QA_TEMP` | Stage 20 | Temporary clone of production where the release is built | Stage 20, after it is swapped into QA |
| `<DATABASE>_QA` | Stage 20 | Stable QA database with the release candidate | Replaced on the next stage 20 run of a later day |
| Blue/green staging database | Stage 30 (`dbt-coves blue-green`) | Building the new production version | Not dropped: it is swapped in and becomes production |
| `<DATABASE>` | Exists before the first deployment | Production | On every deployment the previous production version is swapped out and dropped by `dbt-coves blue-green` |
