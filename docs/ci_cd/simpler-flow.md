# Use a simpler flow: feature to main

> Part of the [CI/CD pipelines](../ci_cd_pipelines.md) documentation.

Not every team needs release branches and a QA stage. To let feature branches go straight to `main`:

1. **`automate/git/branch_validator.py`**: allow `feature` to merge into `main`, for example `"feature": ("main",)`.
2. **Stage 10**: change the `branches:` filter of both `10_feature_*` workflows from `'release/**/*'` to `main`.
3. **Stage 20**: remove `20_release_dbt_checks.yml`, or narrow its trigger, so PRs into `main` are not built twice. Move the *Add deploy marker* and *Remove deploy marker* jobs into stage 10 if you want the `full-refresh` label to reach the production deployment.
4. **Stage 15**: change the `branches:` filter to `main` and remove the `merged == false` condition. A merged PR is deployed right away, so its database is no longer needed.
5. **Stage 30**: the PR database cleanup steps can be removed, because stage 15 already drops every PR database when its PR closes. If you keep them, they are harmless: the targeted cleanup skips deployments that don't come from a release branch, and the sweep drops databases of PRs merged into `main`.

Hotfix and infra branches are not needed in this flow, since every branch already goes to `main`.
