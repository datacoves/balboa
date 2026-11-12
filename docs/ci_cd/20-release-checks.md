# Stage 20: Release checks

> Part of the [CI/CD pipelines](../ci_cd_pipelines.md) documentation.

Stage 20 runs on PRs that target `main` and change `transform/`: release branches, and also hotfix and infra branches, which skip the release branch. It builds all the changes in the release together, against a copy of production, so the release is validated as a whole before it is deployed.

`20_release_dbt_checks.yml` triggers on `pull_request` into `main` for changes in `transform/**`, on the event types `opened`, `synchronize`, `labeled` and `unlabeled`:

- `reopened` is not one of them, so a PR into `main` that is closed and reopened needs a new push to run stage 20 again.
- A PR into `main` that changes nothing under `transform/`, such as an infra PR that only edits a workflow, does not trigger it, so that PR gets no branch validation either.
- The workflow can also be run manually (`workflow_dispatch`), but a manual run has no PR, so the branch validation fails and the dbt job is skipped. Push a commit to the PR instead.

| Job | What it does | Runs when |
|---|---|---|
| Add deploy marker | Appends `[deploy:full-refresh]` to the PR title and posts a comment | The `full-refresh` label is added |
| Remove deploy marker | Removes `[deploy:full-refresh]` from the PR title and posts a comment | The `full-refresh` label is removed |
| [Validate branch](01-branching-and-validation.md#branch-validation) | Checks prefix, merge direction and that the branch is up to date with `main` | Always |
| dbt | Builds the release in the QA database (steps below) | The PR is opened or updated, or the `full-refresh` label is added |
| Status | Posts a PR comment with the result of every job | Always |

> [!WARNING]
> Hotfix and infra branches go straight to `main`, so they never run [stage 10](10-feature-checks.md). Stage 20 builds and tests their dbt changes, but it does not run the governance checks (pre-commit: dbt-checkpoint, SQLFluff, yamllint), the orphaned schemas check or the Airflow checks. A hotfix that only changes `orchestrate/` runs no checks at all: stage 10 only runs for PRs into release branches, and stage 20 only for changes under `transform/`. Review these PRs with that in mind.

## Why a deploy marker in the PR title

[Stage 30](30-deploy-to-production.md) runs after the merge, when the PR and its labels are no longer available to the workflow. It only sees the latest commit on `main`, so writing `[deploy:full-refresh]` in the PR title is how the label's intent travels to the production deployment. Whether the title reaches that commit depends on the merge method and on the default commit messages in **Settings › General › Pull Requests**:

| Merge method | Does the commit message include the PR title? |
|---|---|
| **Create a merge commit** | Yes, with the default message |
| **Squash and merge** | With the default message, only if the PR has more than one commit. A single-commit PR uses that commit's message instead. Set the default squash message to **Pull request title** (or one of the options that start with the title) to always include it |
| **Rebase and merge** | No. The last commit on `main` is a commit from the branch, so production is deployed without `--full-refresh` |

Before merging a PR with the `full-refresh` label, check that the merge commit message GitHub proposes still contains `[deploy:full-refresh]`.

## QA databases

The release is built in a QA database, not in production and not in the feature PR databases:

| Database | Role |
|---|---|
| `<DATABASE>` | Production, the source of the clone. Never modified by this stage |
| `<DATABASE>_QA_TEMP` | Temporary clone where the release is built |
| `<DATABASE>_QA` | Stable QA database that users query to validate the release |

The first run of the day clones production into `_QA_TEMP`, builds the release there, and then swaps it into `_QA`. This keeps the previous `_QA` usable while the build is running, and gives QA a database with fresh production data. If the build fails, the swap does not happen: `_QA` keeps its previous version, and the next run clones production into `_QA_TEMP` again.

Later runs on the same day (for example, after a fix is pushed to the release branch) build directly in `_QA`, which avoids cloning production again. While one of these runs is building, QA users may see a mix of old and new tables in `_QA`.

"Today" is the current date in Snowflake (`CURRENT_DATE()` in the account's time zone), not in the developer's time zone. That is why a run late in the day can clone production again even if another run already did it earlier the same local day.

There is a single `_QA` database for every PR into `main`. If a release PR and a hotfix PR run stage 20 on the same day, both build into the same `_QA`, and it reflects the changes of both.

## dbt steps

| # | Step | What it does | Runs when |
|---|---|---|---|
| 1 | Checkout, set secure directory, list changed files, `dbt deps` | Same as [stage 10](10-feature-checks.md#dbt-checks) | Always |
| 2 | Check if QA was created today | `check_database_created_today` returns `true` when `<DATABASE>_QA` was created today | Always |
| 3 | Choose the target database | Sets `DATACOVES__MAIN__DATABASE` to `<DATABASE>_QA` if it was created today, otherwise to `<DATABASE>_QA_TEMP` | Always |
| 4 | Clone production | `clone_database` drops `<DATABASE>_QA_TEMP` if it exists and recreates it as a zero-copy clone of `<DATABASE>` | QA not created today |
| 5 | Get prod manifest | `get_artifacts.sh` downloads the production manifest for Slim CI | Always |
| 6 | dbt build | `dbt build --fail-fast --defer --state logs --select state:modified+`, or a full build when no manifest is found. Adds `--full-refresh` when the PR has the `full-refresh` label | Always |
| 7 | Swap and drop | `swap_database` swaps `_QA_TEMP` with `_QA` (creating `_QA` the first time), then drops the old database, now named `_QA_TEMP` | QA not created today |
| 8 | Grant access | `grant_db_usage` restores the grants on `<DATABASE>_QA` so QA users can query it | QA not created today |
