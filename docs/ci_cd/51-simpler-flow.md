# Use a simpler flow: feature to main

> Part of the [CI/CD pipelines](../ci_cd_pipelines.md) documentation.

Not every team needs release branches and a QA stage. To let feature branches go straight to `main`:

1. **`automate/git/branch_validator.py`**: allow `feature` to merge into `main`, for example `"feature": ("main",)`. `main` is already in `VALID_TARGET_PREFIXES`.

2. **Stage 10**: change the `branches:` filter of both `10_feature_*` workflows from `'release/**/*'` to `main`.

3. **Stage 20**: remove `20_release_dbt_checks.yml`, or narrow its trigger, so PRs into `main` are not built twice.

   If you want the `full-refresh` label to reach the production deployment, move the *Add deploy marker* and *Remove deploy marker* jobs from `20_release_dbt_checks.yml` into `10_feature_dbt_checks.yml` (not the Airflow workflow), and make these two changes there, otherwise the jobs never run:

   - Add the label events to the trigger. Setting `types:` replaces the default event types, so list those too:

     ```yaml
     on:
       pull_request:
         types: [opened, synchronize, reopened, labeled, unlabeled]
     ```

   - Add an `if` condition to the `dbt` job, so it builds when the `full-refresh` label is added but not when a label is removed or another label is added:

     ```yaml
     if: github.event.action != 'unlabeled' && (github.event.action != 'labeled' || github.event.label.name == 'full-refresh')
     ```

   Optionally, add the two marker jobs to the `needs` of the `dbt-job-status` job, as in stage 20, so the status comment reports them.

   In this flow, the label on a feature PR also reaches production, unlike in the [full flow](40-full-refresh.md#how-it-works-in-each-stage), where stage 10 only refreshes the PR database.

4. **Stage 15**: change the `branches:` filter to `main` and remove the `if:` condition of the `drop-pr-db-on-close` job, so it runs on every closed PR. A merged PR is deployed right away, so its database is no longer needed.

5. **Stage 30**: the [PR database cleanup](31-pr-database-cleanup.md) steps can be removed, because stage 15 already drops every PR database when its PR closes. Without them, the CI role no longer needs `IMPORTED PRIVILEGES` on the `SNOWFLAKE` database. If you keep them, they are harmless: the targeted cleanup skips deployments that don't come from a release branch, and the sweep drops databases of PRs merged into `main`.

## What changes for the team

- **Branches need updating more often.** Every merge into `main` is deployed, and every deployment adds a `Set deployment version … [skip ci]` commit to `main`. Because the branch validator requires the source branch to be [up to date with `main`](01-branching-and-validation.md#branch-validation), every open PR fails validation after each merge until it brings in `main`. The **Update branch** button on the PR does this.
- **There is no QA database.** Without stage 20, `<DATABASE>_QA` is no longer rebuilt. Drop it, and check that nothing still uses it, such as BI tools or grants.
- **Hotfix and infra branches are optional.** Every branch already goes to `main`, so they are no longer needed. You can keep them, or remove them from `BRANCH_RULES` and from the docs and onboarding material that describe them, including the branching section of `AGENTS.md`.
