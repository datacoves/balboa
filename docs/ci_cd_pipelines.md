# CI/CD Pipelines

The workflows in `.github/workflows/` validate every change before it reaches production and then deploy it with zero downtime. This repository implements the full release flow (feature → release → `main`). This documentation explains what each stage does, step by step, and how to customize it, including [how to simplify it](ci_cd/51-simpler-flow.md) for teams that merge feature branches straight into `main`. Start with the stages below, then follow the [documentation map](#documentation-map).

For a higher-level overview of what these pipelines give you, see [CI/CD best practices](https://docs.datacoves.com/docs/best-practices/ci-cd) in the Datacoves docs.

## Numbered stages

Workflow files are prefixed with a number that tells you where in the delivery lifecycle they run. Gaps between numbers are intentional, so you can add your own stages without renaming existing files.

| Stage | Workflow | Runs when | Purpose |
|---|---|---|---|
| **10** | `10_feature_dbt_checks.yml`, `10_feature_airflow_checks.yml` | A pull request (PR) from a feature branch targets a release branch and changes `transform/` (dbt) or `orchestrate/` (Airflow) | Build and test the change in an isolated PR database, run governance checks, validate Airflow DAGs |
| **15** | `15_drop_feature_db.yml` | A PR into a release branch is closed | Drop the PR database when the PR was abandoned |
| **20** | `20_release_dbt_checks.yml` | A PR targets `main` (release, hotfix or infra branch) and changes `transform/` | Build the release in a QA database cloned from production |
| **30** | `30_deploy_changes_to_production.yml` | A commit lands on `main` that changes `transform/`, `automate/` or `.github/workflows/` | Blue/green deploy to production, clean up, publish docs and artifacts, tag the version |

Hotfix and infra branches skip stage 10, so they don't run the governance and Airflow checks. See [Stage 20: Release checks](ci_cd/20-release-checks.md).

Workflows that are specific to this demo, such as `12_feature_cartage_checks.yml` and `99_schedule_image_pandas.yml`, are not covered here.

## Flow at a glance

![CI/CD branch flow: a feature branch PR into a release branch runs stage 10, an abandoned feature PR runs stage 15, a release PR into main runs stage 20, and a merge to main runs stage 30. Hotfix and infra branches open PRs directly into main, which also run stage 20.](diagrams/cicd-branch-flow.png)

## Documentation map

The files in `ci_cd/` are numbered in reading order. The `0x` files cover the concepts and setup, the stage pages use the number of their workflow (10, 15, 20, 30), and the higher numbers cover cross-stage topics and customization.

| I want to... | Read |
|---|---|
| Understand which branches and merges are allowed | [01 Branching and validation](ci_cd/01-branching-and-validation.md) |
| See which database each stage uses | [02 Database lifecycle](ci_cd/02-database-lifecycle.md) |
| Set up variables, secrets and CI images | [03 Configuration](ci_cd/03-configuration.md) |
| Know what runs on a feature PR | [10 Feature checks](ci_cd/10-feature-checks.md) |
| Know when an abandoned PR's database is dropped | [15 Drop PR database](ci_cd/15-drop-pr-database.md) |
| Know what runs on a PR into `main` | [20 Release checks](ci_cd/20-release-checks.md) |
| Know how production is deployed | [30 Deploy to production](ci_cd/30-deploy-to-production.md) |
| Know when merged PRs' databases are dropped | [31 PR database cleanup](ci_cd/31-pr-database-cleanup.md) |
| Rebuild incremental models from scratch | [40 Full refresh](ci_cd/40-full-refresh.md) |
| Change prefixes, paths, images or add a stage | [50 Customize the pipelines](ci_cd/50-customization.md) |
| Merge feature branches straight into `main` | [51 Use a simpler flow](ci_cd/51-simpler-flow.md) |
| Add markers, stricter branch names or other extras | [52 Optional extensions](ci_cd/52-optional-extensions.md) |
