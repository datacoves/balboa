# Branching and validation

> Part of the [CI/CD pipelines](../ci_cd_pipelines.md) documentation.

Which branches the pipelines accept, which merges are allowed, and how the `validate-branch` job enforces it.

## Branching model

![CI/CD branch flow: a feature branch PR into a release branch runs stage 10, an abandoned feature PR runs stage 15, a release PR into main runs stage 20, and a merge to main runs stage 30. Hotfix and infra branches open PRs directly into main, which also run stage 20.](../diagrams/cicd-branch-flow.png)

| Source branch | Can merge into | Workflows that run on the PR |
|---|---|---|
| `feature/*` | `feature/*`, `release/*` | 10, and 15 on close (only when the target is `release/*`) |
| `release/*` | `main` | 20 |
| `hotfix/*` | `main` | 20 |
| `infra/*` | `main` | 20 |

Stages 10 and 20 only run when the PR changes the folders in their `paths` filter (`transform/` for dbt, `orchestrate/` for Airflow). A PR that changes nothing in those folders, such as an infra PR that only edits a workflow, runs no checks.

The prefixes are defined in `automate/git/branch_validator.py`. See [Change branch prefixes](50-customization.md#change-branch-prefixes).

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
| `There are N commit(s) in the main branch that are not in source branch` | The source branch is behind `main`. Every production deployment adds a commit to `main` (`Set deployment version to <version> [skip ci]`, see [Versioning](30-deploy-to-production.md#versioning)), so a branch created before the latest deployment fails this check even if nobody else changed `main` | Merge or rebase `main` into your branch and push |
| `Failed to compare branches via GitHub API` | The GitHub Compare API call failed | Re-run the job. If it persists, check that the workflow's `GITHUB_TOKEN` can read the repository |
