# CI/CD Pipelines

The workflows in `.github/workflows/` validate every change before it reaches production and then deploy it with zero downtime. This repository implements the full release flow (feature → release → `main`). This document explains what each stage does, step by step, and how to customize it, including [how to simplify it](#use-a-simpler-flow-feature-to-main) for teams that merge feature branches straight into `main`.

For a higher-level overview of what these pipelines give you, see [CI/CD best practices](https://docs.datacoves.com/docs/best-practices/ci-cd) in the Datacoves docs.

- [Numbered stages](#numbered-stages)
- [Branching model](#branching-model)
- [Database lifecycle](#database-lifecycle)
- [Required configuration](#required-configuration)
- [Branch validation](#branch-validation)
- [Stage 10: Feature checks](#stage-10-feature-checks)
- [Stage 15: Drop PR database](#stage-15-drop-pr-database)
- [Stage 20: Release checks](#stage-20-release-checks)
- [Stage 30: Deploy to production](#stage-30-deploy-to-production)
- [Customize the pipelines](#customize-the-pipelines), including [a simpler feature-to-main flow](#use-a-simpler-flow-feature-to-main)
- [Optional extensions](#optional-extensions)

## Numbered stages

Workflow files are prefixed with a number that tells you where in the delivery lifecycle they run. Gaps between numbers are intentional, so you can add your own stages without renaming existing files.

| Stage | Workflow | Runs when | Purpose |
|---|---|---|---|
| **10** | `10_feature_dbt_checks.yml`, `10_feature_airflow_checks.yml` | A pull request (PR) from a feature branch targets a release branch | Build and test the change in an isolated PR database, run governance checks, validate Airflow DAGs |
| **15** | `15_drop_feature_db.yml` | A PR into a release branch is closed | Drop the PR database when the PR was abandoned |
| **20** | `20_release_dbt_checks.yml` | A PR targets `main` (release, hotfix or infra branch) | Build the release in a QA database cloned from production |
| **30** | `30_deploy_changes_to_production.yml` | A commit lands on `main` | Blue/green deploy to production, clean up, publish docs and artifacts, tag the version |

Workflows that are specific to this demo, such as `12_feature_cartage_checks.yml` and `99_schedule_image_pandas.yml`, are not covered here.

## Branching model

![CI/CD branch flow: a feature branch PR into a release branch runs stage 10, an abandoned feature PR runs stage 15, a release PR into main runs stage 20, and a merge to main runs stage 30. Hotfix and infra branches open PRs directly into main.](diagrams/cicd-branch-flow.png)

| Source branch | Can merge into | Workflows that run on the PR |
|---|---|---|
| `feature/*` | `feature/*`, `release/*` | 10 (only when the target is `release/*`), 15 on close |
| `release/*` | `main` | 20 |
| `hotfix/*` | `main` | 20 |
| `infra/*` | `main` | 20 |

Stages 10 and 20 only run when the PR changes the folders in their `paths` filter (`transform/` for dbt, `orchestrate/` for Airflow). A PR that changes nothing in those folders, such as an infra PR that only edits a workflow, runs no checks.

The prefixes are defined in `automate/git/branch_validator.py`. See [Change branch prefixes](#change-branch-prefixes).

## Database lifecycle

Every stage works against a different Snowflake database, all derived from the base database name in `DATACOVES__MAIN__DATABASE`. Below, `<DATABASE>` is that base name.

![Database lifecycle: the PR database is created in stage 10, kept while the release is open and dropped after the stage 30 deploy, or in stage 15 if the PR is abandoned. The QA temp database is cloned and built in stage 20 and swapped into QA. The staging database is cloned and built in stage 30 and swapped with production.](diagrams/cicd-database-lifecycle.png)

| Database | Created by | Used for | Dropped by |
|---|---|---|---|
| `<DATABASE>_PR_<pr number>` | Stage 10 | Building and reviewing a single feature PR | Stage 15 if the PR is abandoned; stage 30 after the release that includes it is deployed |
| `<DATABASE>_QA_TEMP` | Stage 20 | Temporary clone of production where the release is built | Stage 20, after it is swapped into QA |
| `<DATABASE>_QA` | Stage 20 | Stable QA database with the release candidate | Replaced on the next stage 20 run of a later day |
| Blue/green staging database | Stage 30 (`dbt-coves blue-green`) | Building the new production version | Not dropped: it is swapped in and becomes production |
| `<DATABASE>` | Exists before the first deployment | Production | On every deployment the previous production version is swapped out and dropped by `dbt-coves blue-green` |

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
| `datacoves/ci-airflow-dbt-snowflake:<version>` | Airflow job in stage 10 | Airflow, the Datacoves Airflow providers, and the `/usr/app/test_dags.py` script |

Pin the image tag to the Datacoves release your environment uses. See [Datacoves versioning](https://docs.datacoves.com/docs/reference/datacoves/versioning) for the available tag formats.

## Branch validation

Before any dbt or Airflow check runs, stages 10 and 20 run a `validate-branch` job. It executes `automate/git/branch_validator.py`, and the dbt and Airflow jobs depend on it: if the validation fails, they are skipped.

The validator runs three checks, in this order:

1. **Source branch prefix.** The PR's source branch must start with one of the allowed prefixes: `feature`, `release`, `hotfix` or `infra`. The target branch must start with `feature`, `release` or `main`.
2. **Merge direction.** Each source prefix can only merge into specific targets.
3. **Up to date with `main`.** The source branch must contain every commit in `main`. This check always compares against `main`, even when the PR targets a release branch, so merge conflicts are resolved by the developer on the feature branch, before they reach the release.

If the source and target branches are the same, validation is skipped.

The rules live in the `BRANCH_RULES` dictionary at the top of the script:

```python
# Allowed merge targets per source branch prefix
BRANCH_RULES = {
    "feature": ("feature", "release"),
    "release": ("main",),
    "hotfix": ("main",),
    "infra": ("main",),
}

VALID_TARGET_PREFIXES = ("feature", "release", "main")
```

Prefixes are matched case-insensitively and longest-first, so a short prefix never shadows a longer one.

The job passes `SOURCE_BRANCH` (`${{ github.head_ref }}`) and `TARGET_BRANCH` (`${{ github.base_ref }}`) to the script, plus `GITHUB_TOKEN` and `GITHUB_REPOSITORY`, because the up-to-date check uses the GitHub Compare API.

### Error messages

| Message | Cause | Fix |
|---|---|---|
| `Source branch must start with one of: ...` | The branch name does not use an allowed prefix | Create a new branch with a valid prefix and open a new PR |
| `Target branch must start with one of: ...` | The PR targets a branch that is not `feature`, `release` or `main` | Change the PR's target branch |
| `'feature' branch can only be merged to: feature, release` | The merge direction is not allowed (for example `feature` into `main`) | Target the correct branch, for example a release branch |
| `There are N commit(s) in the main branch that are not in source branch` | The source branch is behind `main` | Merge or rebase `main` into your branch and push |

## Stage 10: Feature checks

Stage 10 runs when a developer opens or updates a PR from a feature branch into a release branch. It builds the change in an isolated database, enforces governance rules and validates Airflow DAGs, so reviewers only look at code that already works.

There are two workflows with the same number, one per area of the repository:

| Workflow | Runs when the PR changes | What it checks |
|---|---|---|
| `10_feature_dbt_checks.yml` | `transform/**` | dbt build, tests and governance |
| `10_feature_airflow_checks.yml` | `orchestrate/**` | Airflow DAG load time, variable usage and custom DAG tests |

Both workflows:

- Trigger on `pull_request` into `release/**/*` and can also be run manually (`workflow_dispatch`).
- Use a `concurrency` group per PR with `cancel-in-progress: true`, so a new push cancels the run that is still in progress for the same PR.
- Start with the [branch validation](#branch-validation) job. If it fails, the dbt or Airflow job is skipped.
- End with a status job that posts (and updates) a comment on the PR summarizing each job's result.

### dbt checks

The `dbt` job runs in the `datacoves/ci-basic-dbt-snowflake` image, with `transform/` as the working directory and `automate/dbt` as `DBT_PROFILES_DIR`.

The key setting is the database name: the job sets `DATACOVES__MAIN__DATABASE` to `<DATABASE>_PR_<pr number>`, so every model in the PR is built in a database that belongs to that PR only.

Steps run in order and the job stops at the first failure. "Always" in the last column means the step has no extra condition.

| # | Step | What it does | Runs when |
|---|---|---|---|
| 1 | Checkout branch | Checks out the PR head commit with full history (`fetch-depth: 0`) | Always |
| 2 | Set secure directory | Marks the workspace as a safe git directory inside the container | Always |
| 3 | List of files changed | Prints `git diff --name-status` against the target branch, for the log | Always |
| 4 | Install dbt packages | `dbt deps` | Always |
| 5 | Create PR database | `dbt run-operation create_database` creates `<DATABASE>_PR_<n>` if it does not exist yet. Later pushes to the same PR reuse it | Always |
| 6 | Get prod manifest | `automate/dbt/get_artifacts.sh` runs the `get_last_artifacts` macro to download the production `manifest.json` and `catalog.json` from a Snowflake stage into `logs/`. Sets the outputs `manifest_found` and `catalog_found` | Always |
| 7 | Clone incremental models | `dbt clone` of incremental models and snapshots that are downstream of the change and already exist in production (`state:modified+,config.materialized:incremental,state:old`), so they keep their history instead of being rebuilt from scratch | Manifest found |
| 8 | Governance build with empty models | `dbt build --empty` creates every affected model with no rows. This is enough to compile the SQL and run hooks, and it fails fast before spending warehouse time. Uses `--defer --state logs --select state:modified+` when the manifest exists, or the whole project when it does not | Always |
| 9 | Generate docs | `dbt-coves generate docs --merge-deferred --state logs` merges the production catalog with the PR catalog, or `dbt docs generate` when no production catalog exists. The catalog is needed by the governance checks | Always |
| 10 | Run governance checks | `pre-commit run --from-ref origin/<target> --to-ref HEAD` runs the hooks in `.pre-commit-config.yaml` only on the files changed in the PR: [dbt-checkpoint](https://github.com/dbt-checkpoint/dbt-checkpoint) (descriptions, property files, columns), [SQLFluff](https://docs.sqlfluff.com/) and yamllint | Always |
| 11 | dbt build | The real `dbt build --fail-fast`: Slim CI (`--defer --state logs --select state:modified+`) when the manifest exists, or a full build when it does not | Always |
| 12 | Check orphaned schemas | `automate/dbt/orphaned_schemas.py` compares the production manifest with the PR manifest and reports schemas that dbt will stop managing once the PR is deployed. The result is posted as a PR comment | Manifest found, even if a previous step failed |
| 13 | Grant access to PR database | `grant_access_to_pr_database` grants usage on the database and its schemas, and select on its tables and views, so reviewers can query the PR database | Always |
| 14 | Drop PR database on failure | Drops the PR database if the grants failed. A failure here usually means models were built into the wrong schema | `DATACOVES__DROP_DB_ON_FAIL` is `true` and step 13 failed |

The workflow also contains steps that are specific to this demo project, such as building an observability model first and building dynamic tables separately (`--empty` does not support them).

#### Slim CI and the production manifest

Steps 7, 8 and 11 rely on the production `manifest.json` that [stage 30](#stage-30-deploy-to-production) uploads after every deployment. With it, dbt builds only what changed and its downstream models, and **defers** every unchanged upstream reference to the production database. Without it (for example, on the very first run), the job falls back to building the whole project.

#### Full refresh

By default, incremental models in the PR database are cloned from production (step 7) and then built incrementally. When a change requires rebuilding them from scratch, for example a new column, a changed `unique_key` or a fix to historical logic, add the `full-refresh` label to the PR.

The workflow checks for the label in a job-level variable:

```yaml
env:
  FULL_REFRESH_FLAG: ${{ contains(github.event.pull_request.labels.*.name, 'full-refresh') && '--full-refresh' || '' }}
```

and appends it to both `dbt build` commands (steps 8 and 11). With the label, step 11 becomes:

```bash
dbt build --fail-fast --defer --state logs --select state:modified+ --full-refresh
```

The flag applies to the same selection as a normal run: in Slim CI, only the changed models and their downstream models are rebuilt, not the whole project.

To run a full refresh on a feature PR:

1. **Create the label once per repository.** Go to **Issues › Labels › New label** and name it exactly `full-refresh`, or run:

   ```bash
   gh label create full-refresh --description "Rebuild incremental models with dbt --full-refresh"
   ```

2. **Add the label to the PR.** Use **Labels** in the PR sidebar, or run:

   ```bash
   gh pr edit <pr number> --add-label full-refresh
   ```

3. **Start a new run with a push.** Stage 10 does not trigger on label events, and re-running a previous run from the **Actions** tab does not help either: GitHub re-runs with the original event data, which does not include the new label. Push a new commit; if you have nothing to change, an empty commit works:

   ```bash
   git commit --allow-empty -m "Trigger full refresh"
   git push
   ```

4. **Check the log.** In the run, the `dbt build` steps should end with `--full-refresh`.

The label stays on the PR, so every later push also runs a full refresh. Remove it (`gh pr edit <pr number> --remove-label full-refresh`) once the PR database has been rebuilt, if the following pushes don't need it.

Where the label has an effect:

| Where you add the label | Effect |
|---|---|
| Feature PR into a release branch (stage 10) | Full refresh in that PR's database only. It never reaches production |
| Release PR into `main` ([stage 20](#stage-20-release-checks)) | Full refresh in the QA database, plus the `[deploy:full-refresh]` marker in the PR title. Stage 20 does react to the label, so adding it starts a run |
| Merge into `main` ([stage 30](#stage-30-deploy-to-production)) | The production deployment runs `--full-refresh` when the merge commit contains `[deploy:full-refresh]`. See [Why a deploy marker in the PR title](#why-a-deploy-marker-in-the-pr-title) |

To refresh only some models instead of the whole selection, see [Commit markers instead of labels](#commit-markers-instead-of-labels).

### Airflow checks

The `airflow` job runs in the `datacoves/ci-airflow-dbt-snowflake` image, with `AIRFLOW__CORE__DAGS_FOLDER` pointing at `orchestrate/dags`.

| # | Step | What it does |
|---|---|---|
| 1 | Checkout branch | Checks out the PR head commit |
| 2 | Test DAG load time and variable usage | Runs `/usr/app/test_dags.py`, included in the image, with `--dag-loadtime-threshold 1` (fails DAGs that take more than one second to parse) and `--check-variable-usage` (fails on `Variable.get()` calls at the top level of a DAG file, outside `@task` functions). `--write-output` writes the results to a Markdown file |
| 3 | Comment results | Posts the load-time results as a PR comment when step 2 succeeds |
| 4 | Custom Airflow validation tests | Runs `pytest` on `orchestrate/test_dags/validate_dags.py`, your own DAG tests. They check for import errors, approved tags (`APPROVED_TAGS`), `catchup=False`, and allowed operators (`ALLOWED_OPERATORS`) |
| 5 | Comment results | Posts the custom test results as a PR comment, even when the tests fail |

See [Test Airflow DAGs in your CI/CD pipeline](https://docs.datacoves.com/docs/how-tos/airflow/dags/test-dags) for how to write the custom tests.

## Stage 15: Drop PR database

Every feature PR gets its own database, `<DATABASE>_PR_<pr number>`, created by [stage 10](#stage-10-feature-checks). Stage 15 removes that database when the PR was **abandoned**.

| PR outcome | What happens to `<DATABASE>_PR_<n>` |
|---|---|
| Closed **without merging** (abandoned) | Stage 15 drops it immediately |
| **Merged** into the release branch | Stage 15 does nothing. The database is kept so QA can keep querying it while the release is being validated, and [stage 30](#pr-database-cleanup) drops it once the release that includes the PR is deployed to production |
| Still open | Kept |

`15_drop_feature_db.yml` triggers on `pull_request` events of type `closed` for PRs into `release/**/*`. GitHub sends the same event for merged and abandoned PRs, so the job checks the `merged` flag of the event:

```yaml
drop-pr-db-on-close:
  if: github.event_name == 'workflow_dispatch' || github.event.pull_request.merged == false
```

The job runs in the `datacoves/ci-basic-dbt-snowflake` image with `DATACOVES__MAIN__DATABASE` set to `<DATABASE>_PR_<n>`.

| # | Step | What it does |
|---|---|---|
| 1 | Checkout branch | Checks out the PR head commit, or the commit the workflow was started from in a manual run |
| 2 | Set secure directory | Marks the workspace as a safe git directory inside the container |
| 3 | Install dbt packages | `dbt deps` |
| 4 | Validate PR number | Fails the job if the PR number is empty or not numeric, so the drop never runs against an incomplete database name |
| 5 | Drop PR database | `dbt run-operation drop_recreate_db --args "{db_name: <DATABASE>_PR_<n>, recreate: False}"` |

### Run it manually

Manual runs (`workflow_dispatch`) skip the `merged` check and always drop the database of the PR you specify. A manual run has no PR event, so the workflow takes the PR number from a required `pr_number` input:

```yaml
workflow_dispatch:
  inputs:
    pr_number:
      description: "PR number whose test database should be dropped"
      required: true
      type: number
```

The database name uses the event's PR number and falls back to the input:

```yaml
DATACOVES__MAIN__DATABASE: ${{ vars.DATACOVES__MAIN__DATABASE }}_PR_${{ github.event.number || inputs.pr_number }}
```

To drop a PR database by hand, open **Actions**, select the **15: 🗑️ Drop test databases** workflow, click **Run workflow**, enter the PR number and run it. You can also use the GitHub CLI:

```bash
gh workflow run 15_drop_feature_db.yml -f pr_number=<pr number>
```

Use it for databases that were left behind, for example when a closed PR's run failed. It drops the database regardless of the PR's state, so don't use it for a PR whose release is still in QA.

## Stage 20: Release checks

Stage 20 runs on every PR that targets `main`: release branches, and also hotfix and infra branches, which skip the release branch. It builds all the changes in the release together, against a copy of production, so the release is validated as a whole before it is deployed.

`20_release_dbt_checks.yml` triggers on `pull_request` into `main` for changes in `transform/**`, on the event types `opened`, `synchronize`, `labeled` and `unlabeled`. It can also be run manually. A PR into `main` that changes nothing under `transform/`, such as an infra PR that only edits a workflow, does not trigger it, so that PR gets no branch validation either.

| Job | What it does | Runs when |
|---|---|---|
| Add deploy marker | Appends `[deploy:full-refresh]` to the PR title and posts a comment | The `full-refresh` label is added |
| Remove deploy marker | Removes `[deploy:full-refresh]` from the PR title and posts a comment | The `full-refresh` label is removed |
| [Validate branch](#branch-validation) | Checks prefix, merge direction and that the branch is up to date with `main` | Always |
| dbt | Builds the release in the QA database (steps below) | The PR is opened or updated, or the `full-refresh` label is added |
| Status | Posts a PR comment with the result of every job | Always |

### Why a deploy marker in the PR title

[Stage 30](#stage-30-deploy-to-production) runs after the merge, when the PR and its labels are no longer available to the workflow. It only sees the merge commit. With GitHub's default merge settings, the PR title is part of the merge commit message (and is the whole message of a squash merge), so writing `[deploy:full-refresh]` in the title is how the label's intent travels to the production deployment. If you customize the default merge or squash commit message, make sure it still includes the PR title.

### QA databases

The release is built in a QA database, not in production and not in the feature PR databases:

| Database | Role |
|---|---|
| `<DATABASE>` | Production, the source of the clone. Never modified by this stage |
| `<DATABASE>_QA_TEMP` | Temporary clone where the release is built |
| `<DATABASE>_QA` | Stable QA database that users query to validate the release |

The first run of the day clones production into `_QA_TEMP`, builds the release there, and then swaps it into `_QA`. This keeps `_QA` usable while the build is running, and gives QA a database with fresh production data. Later runs on the same day (for example, after a fix is pushed to the release branch) build directly in `_QA`, which avoids cloning production again.

There is a single `_QA` database for every PR into `main`. If a release PR and a hotfix PR run stage 20 on the same day, both build into the same `_QA`, and it reflects the changes of both.

### dbt steps

| # | Step | What it does | Runs when |
|---|---|---|---|
| 1 | Checkout, set secure directory, list changed files, `dbt deps` | Same as [stage 10](#dbt-checks) | Always |
| 2 | Check if QA was created today | `check_database_created_today` returns `true` when `<DATABASE>_QA` was created today | Always |
| 3 | Choose the target database | Sets `DATACOVES__MAIN__DATABASE` to `<DATABASE>_QA` if it was created today, otherwise to `<DATABASE>_QA_TEMP` | Always |
| 4 | Clone production | `clone_database` drops `<DATABASE>_QA_TEMP` if it exists and recreates it as a zero-copy clone of `<DATABASE>` | QA not created today |
| 5 | Get prod manifest | `get_artifacts.sh` downloads the production manifest for Slim CI | Always |
| 6 | dbt build | `dbt build --fail-fast --defer --state logs --select state:modified+`, or a full build when no manifest is found. Adds `--full-refresh` when the PR has the `full-refresh` label | Always |
| 7 | Swap and drop | `swap_database` swaps `_QA_TEMP` with `_QA` (creating `_QA` the first time), then drops the old database, now named `_QA_TEMP` | QA not created today |
| 8 | Grant access | `grant_db_usage` restores the grants on `<DATABASE>_QA` so QA users can query it | QA not created today |

## Stage 30: Deploy to production

`30_deploy_changes_to_production.yml` triggers on `push` to `main` when files change in `.github/workflows/**`, `automate/**` or `transform/**`. It can also be run manually.

The job runs in the `datacoves/ci-basic-dbt-snowflake` image with `DATACOVES__MAIN__DATABASE` set to the production database.

| # | Step | What it does |
|---|---|---|
| 1 | Checkout, set secure directory, `dbt deps` | Checks out `main` with full history and installs packages |
| 2 | Get prod manifest | `get_artifacts.sh` downloads the manifest of the **previous** deployment, used to deploy only what changed |
| 3 | Check for full-refresh marker | Looks for `[deploy:full-refresh]` in the latest commit message (added by [stage 20](#why-a-deploy-marker-in-the-pr-title)). If found, the deployment uses `--full-refresh` |
| 4 | Set deployment version and tag | `automate/dbt/set_deployment_version.sh` (see [Versioning](#versioning)) |
| 5 | Blue/green deployment | `dbt-coves blue-green --defer` when a production manifest exists, or `dbt-coves blue-green` for a full build when it does not (see [Blue/green deployment](#bluegreen-deployment)) |
| 6 | Restore grants | `grant_db_usage` restores the select grants on tables, views and dynamic tables that are not carried across the database swap |
| 7 | Drop orphaned relations | `drop_orphaned_relations` drops tables and views that still exist in production but are no longer defined in dbt |
| 8 | Drop orphaned schemas | `orphaned_schemas.py` compares the previous production manifest with the new one to find schemas whose models were all deleted, and `drop_orphaned_schemas` drops them. Only schemas that dbt created are considered, and schemas that contain objects dbt did not build are kept |
| 9 | Generate and publish docs | `dbt docs generate`, then publishes `transform/target` to the `dbt-docs` branch, which Datacoves serves as the project's dbt docs |
| 10 | Upload dbt artifacts | `upload_artifacts` uploads `manifest.json`, `run_results.json` and `catalog.json` to the Snowflake stage, where stages 10, 20 and the next deployment read them |
| 11 | Push version changes | Pushes the version commit and tag created in step 4 to `main` |
| 12 | PR database cleanup | Drops the feature PR databases that are no longer needed (see [PR database cleanup](#pr-database-cleanup)) |

The workflow also includes steps that belong to this demo, such as deploying a Streamlit app, uploading artifacts to S3 for DataHub, and pushing artifacts to the Datacoves dbt API.

### Blue/green deployment

`dbt-coves blue-green` builds the new version of production next to the current one, and swaps them only when everything succeeded:

1. Creates a staging database (by default `<DATABASE>_STAGING`) and clones production's schemas and grants into it.
2. Runs `dbt build` against the staging database. With `--defer`, only modified models and their descendants are built.
3. Copies the database-level grants from production to staging.
4. Swaps staging and production with `ALTER DATABASE ... SWAP WITH ...`.
5. Drops the previous production database, which now has the staging name.

If any model or test fails, the swap never happens and production is not touched. The failed staging database is left in place for debugging, unless `--drop-staging-db-on-failure` is used.

> [!WARNING]
> While a failed staging database exists, the next `dbt-coves blue-green` run stops with an error because the staging database already exists. Drop it before re-running the deployment, or add `--drop-staging-db-at-start` or `--drop-staging-db-on-failure` to the command.

See the [`dbt-coves blue-green` reference](https://github.com/datacoves/dbt-coves/tree/main/docs/commands/blue-green) for all options, such as `--staging-database` to use a fixed staging database name and `--keep-staging-db-on-success` to keep the previous production version as a rollback copy.

### Versioning

Every deployment gets a version in the format `YYYYMMDD.N.0`, where `N` is the deployment number for that day. For example, the second deployment on August 1, 2026 is `20260801.2.0`.

`set_deployment_version.sh`:

1. Fetches existing tags and finds the highest `N` already used today.
2. Writes the new version to the `version:` key of `dbt_project.yml`. The trailing `.0` keeps it a valid three-part semantic version, as dbt requires.
3. Commits the change and creates a git tag with the same name. Both include `[skip ci]` so the push does not trigger stage 30 again.

The version is bumped **before** any step writes dbt artifacts, because dbt stores a hash of `dbt_project.yml` in `partial_parse.msgpack`. Artifacts built before the bump could not be reused against the version that ends up on `main`.

### PR database cleanup

Feature PR databases (`<DATABASE>_PR_<n>`) are kept after the PR is merged into a release branch, so QA can still query them while the release is validated. Once the release is in production they are no longer needed. Stage 30 removes them in two passes. Both run only after a successful deployment, use `continue-on-error`, and never fail the deployment: anything left behind is retried on the next deployment.

#### 1. Targeted cleanup

`automate/git/get_feature_pr_ids.py`:

1. Finds the PR that produced the commit being deployed, using the GitHub API, so it works with merge commits, squash merges and custom commit messages.
2. If that PR came from a release branch, lists every feature PR that was merged into that release branch.
3. Returns their IDs, and the next step drops each `<DATABASE>_PR_<id>`.

Deployments from hotfix or infra branches are skipped here and handled by the sweep.

#### 2. Sweep

The sweep catches everything the targeted cleanup misses: hotfix and infra deployments, abandoned releases, branches that do not follow the naming convention, and drops that failed on a previous run.

1. The `show_pr_databases` macro lists every `<DATABASE>_PR_*` database with its creation date and the date it was last queried. The last query comes from `SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY` and only counts queries that ran with the PR database as the session's current database. Queries by the user that runs the deployment are excluded, so CI builds do not count as activity. The role that runs the deployment needs `IMPORTED PRIVILEGES` on the `SNOWFLAKE` database.
2. `automate/git/sweep_stale_pr_databases.py` checks the PR behind each database and decides whether to drop it.
3. The next step drops the selected databases.

Decision per database:

| PR state | Decision |
|---|---|
| Abandoned (closed without merging) | Drop |
| Not found (deleted) | Drop |
| Open | Keep |
| Merged into a branch that is not a release branch | Drop |
| Merged into a release branch | Depends on the release, see below |
| Any other API error | Keep and retry on the next deployment |

For PRs merged into a release branch, the release rules are evaluated in order, and the first match wins:

| # | Release state | Decision |
|---|---|---|
| 1 | The release has an **open** PR to `main` | Keep (release in QA) |
| 2 | The release has a **merged** PR to `main` | Drop (release shipped) |
| 3 | The release has an **abandoned** PR to `main` | Drop (release abandoned) |
| 4 | The release branch exists but has no PR to `main` yet | Keep, subject to the age limits below |
| 5 | The release branch was deleted and has no PR to `main` | Drop (release discarded) |

Rule 1 is evaluated first because a release PR can be abandoned and reopened. Rule 4 has age limits because release branches are not deleted after merging, so a release that never gets a PR to `main` would otherwise keep its PR databases forever:

| Setting | Default | Meaning |
|---|---|---|
| `PR_MAX_AGE_DAYS` | `15` | Databases up to this age are always kept |
| `PR_MAX_IDLE_DAYS` | `15` | Older databases are kept only if someone queried them in this many days |
| `PR_HARD_MAX_AGE_DAYS` | `45` | Databases older than this are dropped even if they are in use |

The settings are environment variables of the sweep step. An invalid value aborts the sweep without dropping anything.

## Customize the pipelines

The workflows are plain YAML files and scripts in this repository, so every rule can be changed. This section lists the most common changes and every file you need to touch for each one.

| Folder | Contents |
|---|---|
| `.github/workflows/` | Workflow definitions, one file per stage |
| `automate/git/` | Scripts that talk to git and the GitHub API: branch validation, PR database cleanup |
| `automate/dbt/` | Scripts that run around dbt: artifacts, versioning, orphaned schemas, `profiles.yml` |
| `transform/macros/tooling/` | dbt macros used by the workflows: database creation, clone, swap, grants, artifacts |

### Change branch prefixes

To use different prefixes, for example `fb/`, `rb/` and `hf/` instead of `feature/`, `release/` and `hotfix/`, change them in **all** of these places. If you only change the validator, the workflows will stop triggering for the new branch names.

1. **`automate/git/branch_validator.py`**: update the keys and allowed targets in `BRANCH_RULES`, and the list in `VALID_TARGET_PREFIXES`:

   ```python
   BRANCH_RULES = {
       "fb": ("fb", "rb"),
       "rb": ("main",),
       "hf": ("main",),
       "infra": ("main",),
   }

   VALID_TARGET_PREFIXES = ("fb", "rb", "main")
   ```

2. **Workflow triggers**: the `branches:` filter of stages 10 and 15, for example `- 'rb/**/*'` instead of `- 'release/**/*'`.

3. **PR database cleanup scripts** used by [stage 30](#pr-database-cleanup):
   - `automate/git/get_feature_pr_ids.py`: the `release/` check of the deployed branch and the `feature/` filter of the PRs merged into it.
   - `automate/git/sweep_stale_pr_databases.py`: the `release/` check of the branch each PR was merged into.

4. **Docs and onboarding material** that tell developers how to name branches, including the branching section of `AGENTS.md`.

> [!TIP]
> Prefix matching in `branch_validator.py` does not include the `/`, so a branch named `featurex` passes as `feature`. If you need stricter names, see [Enforce a branch name format](#enforce-a-branch-name-format).

### Use a simpler flow: feature to main

Not every team needs release branches and a QA stage. To let feature branches go straight to `main`:

1. **`automate/git/branch_validator.py`**: allow `feature` to merge into `main`, for example `"feature": ("main",)`.
2. **Stage 10**: change the `branches:` filter of both `10_feature_*` workflows from `'release/**/*'` to `main`.
3. **Stage 20**: remove `20_release_dbt_checks.yml`, or narrow its trigger, so PRs into `main` are not built twice. Move the *Add deploy marker* and *Remove deploy marker* jobs into stage 10 if you want the `full-refresh` label to reach the production deployment.
4. **Stage 15**: change the `branches:` filter to `main` and remove the `merged == false` condition. A merged PR is deployed right away, so its database is no longer needed.
5. **Stage 30**: the PR database cleanup steps can be removed, because stage 15 already drops every PR database when its PR closes. If you keep them, they are harmless: the targeted cleanup skips deployments that don't come from a release branch, and the sweep drops databases of PRs merged into `main`.

Hotfix and infra branches are not needed in this flow, since every branch already goes to `main`.

### Change which merges are allowed

Edit the tuple of allowed targets for each prefix in `BRANCH_RULES`. For example, to stop feature branches from merging into other feature branches:

```python
"feature": ("release",),
```

To add a new branch type, add it to `BRANCH_RULES`, and to `VALID_TARGET_PREFIXES` if other branches can merge into it. Then decide which stage runs for its PRs: stage 10 runs for PRs into release branches, and stage 20 for PRs into `main`.

### Tune PR database cleanup

The sweep thresholds are environment variables of the `Sweep: identify orphaned PR databases` step in stage 30:

```yaml
env:
  PR_MAX_AGE_DAYS: "15"       # always keep databases up to this age
  PR_MAX_IDLE_DAYS: "15"      # after that, keep only if queried in this many days
  PR_HARD_MAX_AGE_DAYS: "45"  # drop past this age, even if in use
```

See [Sweep](#2-sweep) for the full decision rules. If QA does not need the feature PR databases during the release, you can drop them as soon as the PR is closed, merged or not, by removing the `merged == false` condition from [stage 15](#stage-15-drop-pr-database).

### Change the trigger paths

Each workflow only runs when certain folders change. If your repository uses a different layout, update the `paths:` filters:

| Stage | Filter |
|---|---|
| 10 dbt | `paths: - transform/**/*` |
| 10 Airflow | `paths: - orchestrate/**/*` |
| 20 | `paths: - transform/**/*` |
| 30 | `paths:` under `on: push` |

A push that changes no files in those paths, such as an empty commit to `main`, does not trigger the workflow. See [Trigger CI/CD without code changes](https://docs.datacoves.com/docs/how-tos/dataops/trigger-cicd-workflow).

### Update the CI images

All dbt jobs use `datacoves/ci-basic-dbt-snowflake`, and the Airflow job uses `datacoves/ci-airflow-dbt-snowflake`. When your Datacoves environment is upgraded, update the image tag in every workflow file so CI runs the same dbt and Airflow versions as your developers. Also keep the dbt versions in the SQLFluff hook of `.pre-commit-config.yaml` aligned with the image.

### Adjust the Airflow checks

- **DAG load time:** change `--dag-loadtime-threshold` (in seconds) in the `test_dags.py` step of `10_feature_airflow_checks.yml`.
- **Variable usage:** remove `--check-variable-usage` to allow `Variable.get()` outside `@task` functions. Keeping it is recommended, because top-level variable reads slow down DAG parsing.
- **Custom rules:** edit `orchestrate/test_dags/validate_dags.py`, for example the `APPROVED_TAGS` set and the `ALLOWED_OPERATORS` list.

### Add a new stage

Pick a number in the gap that matches when the new workflow should run, so the folder keeps reading in lifecycle order:

| Range | Use for | Example |
|---|---|---|
| 10-14 | Checks on feature PRs into a release branch | `12_feature_python_checks.yml` for a loader codebase |
| 15-19 | Cleanup when a feature PR closes | |
| 20-29 | Checks on PRs into `main` | `25_release_security_checks.yml` |
| 30-39 | Deployment and post-deployment steps on `main` | `35_refresh_bi_extracts.yml` |
| 90-99 | Scheduled or independent jobs | `99_build_custom_image.yml` |

For checks on PRs, add the [branch validation](#branch-validation) job first and make the other jobs depend on it, as the existing stages do.

## Optional extensions

These extensions are not implemented in this repository. They come from teams that needed them.

### Commit markers instead of labels

Instead of the `full-refresh` PR label, developers write markers in their commit messages:

| Marker | Effect |
|---|---|
| `[full-refresh]` | Full-refresh build of the whole project |
| `[full-refresh: model_a, model_b]` | Full refresh of only the listed models. Skip the models that do not exist in production yet, because they are built from scratch anyway |
| `[fresh-db]` | Drops and recreates the PR database before building. Read it only from the commits pushed since the previous run, so it fires once instead of on every push |

The workflows need a script that parses the markers from the PR's commits and sets them as step outputs, so the `--full-refresh` steps can be gated on them. Also copy the markers into the PR description: that way they survive a squash merge.

- **Stage 20** parses them from the commits of the release PR and applies the same full or targeted refresh to the QA build.
- **Stage 30** reads them from every commit since the last deployment tag. It refreshes the targeted models before the blue/green run and excludes them from it.

### Wider selection with @state:modified

Select `@state:modified` instead of `state:modified+`. The `@` operator builds the changed models, their descendants, **and every parent of those descendants**. Relationship tests then compare tables that were built in the same run, instead of comparing a freshly built child against an older production parent. The cost is a larger build.

In stage 30, pass it to the blue/green build with `dbt-coves blue-green --dbt-selector="--defer --state logs -s @state:modified"`.

### Enforce a branch name format

The validator only checks prefixes. Some teams also want every branch name to carry a ticket, a date and an owner. To do that, validate the **full branch name** with one regular expression per prefix, plus a human-readable format and an example that the error message shows. For example, with short prefixes:

```python
BRANCH_PATTERNS = {
    "fb": {
        "regex": re.compile(r"^fb/.+_\d{8}_[a-zA-Z]+$", re.IGNORECASE),
        "format": "fb/<jira or project>_yyyymmdd_<initials>",
        "example": "fb/PROJ-123_20250527_jd",
        "allowed_targets": ("fb", "rb"),
    },
    "rb": {
        "regex": re.compile(r"^rb/\d{2}-\d{8}$", re.IGNORECASE),
        "format": "rb/<two digits>-yyyymmdd",
        "example": "rb/01-20250527",
        "allowed_targets": ("main",),
    },
    # hf and infra follow the same structure
}
```

Check the format on both the source and the target branch, and also check that the `yyyymmdd` part is a valid date. To switch to short prefixes like these, see [Change branch prefixes](#change-branch-prefixes).

### Block dbt changes from infra branches

Infra branches go straight to `main` and are meant for workflow and tooling changes. To make sure that no dbt model change skips the release process, add a step to the validation job of stage 20 that fails the PR when an infra branch changes anything under `transform/`:

```bash
CHANGED=$(git diff --name-only origin/$TARGET_BRANCH HEAD -- transform/ ':(exclude)transform/macros/tooling/')
if [ -n "$CHANGED" ]; then
  echo "ERROR: infra branches must not modify the transform/ folder (except transform/macros/tooling/)."
  exit 1
fi
```

The check excludes `transform/macros/tooling/`, which holds the dbt macros that the workflows themselves use, such as database creation, clone, swap and PR database cleanup. That way an infra PR can change a workflow and the macro it calls together. Every other change under `transform/`, such as models, other macros or `dbt_project.yml`, still has to go through a feature branch and a release.

Run this step only when `github.head_ref` starts with `infra/`. Because stage 20 only triggers on changes under `transform/`, it runs on exactly the infra PRs this step has to check.
