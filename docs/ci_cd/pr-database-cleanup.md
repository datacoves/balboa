# PR database cleanup

> Part of the [CI/CD pipelines](../ci_cd_pipelines.md) documentation.

Feature PR databases (`<DATABASE>_PR_<n>`) are kept after the PR is merged into a release branch, so QA can still query them while the release is validated. Once the release is in production they are no longer needed. Stage 30 removes them in two passes. Both run only after a successful deployment, use `continue-on-error`, and never fail the deployment: anything left behind is retried on the next deployment.

## 1. Targeted cleanup

`automate/git/get_feature_pr_ids.py`:

1. Finds the PR that produced the commit being deployed, using the GitHub API, so it works with merge commits, squash merges and custom commit messages.
2. If that PR came from a release branch, lists every feature PR that was merged into that release branch.
3. Returns their IDs, and the next step drops each `<DATABASE>_PR_<id>`.

Deployments from hotfix or infra branches are skipped here and handled by the sweep.

## 2. Sweep

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
| `PR_MAX_AGE_DAYS` | `15` | Databases up to this age (in days) are always kept |
| `PR_MAX_IDLE_DAYS` | `15` | Older databases are kept only if someone queried them in this many days |
| `PR_HARD_MAX_AGE_DAYS` | `45` | Databases older than this are dropped even if they are in use |

The age limits only apply to rule 4. If a database's age cannot be determined, it is kept. The settings are environment variables of the sweep step, and an invalid value aborts the sweep without dropping anything.

## Tune PR database cleanup

The sweep thresholds are environment variables of the `Sweep: identify orphaned PR databases` step in stage 30:

```yaml
env:
  PR_MAX_AGE_DAYS: "15"       # always keep databases up to this age
  PR_MAX_IDLE_DAYS: "15"      # after that, keep only if queried in this many days
  PR_HARD_MAX_AGE_DAYS: "45"  # drop past this age, even if in use
```

See [Sweep](#2-sweep) for the full decision rules. If QA does not need the feature PR databases during the release, you can drop them as soon as the PR is closed, merged or not, by removing the `merged == false` condition from [stage 15](stage-15-drop-pr-database.md).
