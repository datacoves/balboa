# Stage 15: Drop PR database

> Part of the [CI/CD pipelines](../ci_cd_pipelines.md) documentation.

Every feature PR gets its own database, `<DATABASE>_PR_<pr number>`, created by [stage 10](stage-10-feature-checks.md). Stage 15 removes that database when the PR was **abandoned**.

| PR outcome | What happens to `<DATABASE>_PR_<n>` |
|---|---|
| Closed **without merging** (abandoned) | Stage 15 drops it immediately |
| **Merged** into the release branch | Stage 15 does nothing. The database is kept so QA can keep querying it while the release is being validated, and [stage 30](pr-database-cleanup.md) drops it once the release that includes the PR is deployed to production |
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

## Run it manually

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
