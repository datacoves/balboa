# Customize the pipelines

> Part of the [CI/CD pipelines](../ci_cd_pipelines.md) documentation.

The workflows are plain YAML files and scripts in this repository, so every rule can be changed. This section lists the most common changes and every file you need to touch for each one.

| Folder | Contents |
|---|---|
| `.github/workflows/` | Workflow definitions, one file per stage |
| `automate/git/` | Scripts that talk to git and the GitHub API: branch validation, PR database cleanup |
| `automate/dbt/` | Scripts that run around dbt: artifacts, versioning, orphaned schemas, `profiles.yml` |
| `transform/macros/tooling/` | dbt macros used by the workflows: database creation, clone, swap, grants, artifacts |

## Change branch prefixes

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

3. **PR database cleanup scripts** used by [stage 30](pr-database-cleanup.md):
   - `automate/git/get_feature_pr_ids.py`: the `release/` check of the deployed branch and the `feature/` filter of the PRs merged into it.
   - `automate/git/sweep_stale_pr_databases.py`: the `release/` check of the branch each PR was merged into.

4. **Docs and onboarding material** that tell developers how to name branches, including the branching section of `AGENTS.md`.

> [!TIP]
> Prefix matching in `branch_validator.py` does not include the `/`, so a branch named `featurex` passes as `feature`. If you need stricter names, see [Enforce a branch name format](optional-extensions.md#enforce-a-branch-name-format).

## Change which merges are allowed

Edit the tuple of allowed targets for each prefix in `BRANCH_RULES`. For example, to stop feature branches from merging into other feature branches:

```python
"feature": ("release",),
```

To add a new branch type, add it to `BRANCH_RULES`, and to `VALID_TARGET_PREFIXES` if other branches can merge into it. Then decide which stage runs for its PRs: stage 10 runs for PRs into release branches, and stage 20 for PRs into `main`.

## Change the trigger paths

Each workflow only runs when certain folders change. If your repository uses a different layout, update the `paths:` filters:

| Stage | Filter |
|---|---|
| 10 dbt | `paths: - transform/**/*` |
| 10 Airflow | `paths: - orchestrate/**/*` |
| 20 | `paths: - transform/**/*` |
| 30 | `paths:` under `on: push` |

A push that changes no files in those paths, such as an empty commit to `main`, does not trigger the workflow. See [Trigger CI/CD without code changes](https://docs.datacoves.com/docs/how-tos/dataops/trigger-cicd-workflow).

## Update the CI images

All dbt jobs use `datacoves/ci-basic-dbt-snowflake`, and the Airflow job uses `datacoves/ci-airflow-v3-dbt-snowflake`. When your Datacoves environment is upgraded, update the image tag in every workflow file so CI runs the same dbt and Airflow versions as your developers. Also keep the dbt versions in the SQLFluff hook of `.pre-commit-config.yaml` aligned with the image.

## Adjust the Airflow checks

- **DAG load time:** change `--dag-loadtime-threshold` (in seconds) in the `test_dags.py` step of `10_feature_airflow_checks.yml`.
- **Variable usage:** remove `--check-variable-usage` to allow `Variable.get()` outside `@task` functions. Keeping it is recommended, because top-level variable reads slow down DAG parsing.
- **Custom rules:** edit `orchestrate/test_dags/validate_dags.py`, for example the `APPROVED_TAGS` set and the `ALLOWED_OPERATORS` list.

## Add a new stage

Pick a number in the gap that matches when the new workflow should run, so the folder keeps reading in lifecycle order:

| Range | Use for | Example |
|---|---|---|
| 10-14 | Checks on feature PRs into a release branch | `12_feature_python_checks.yml` for a loader codebase |
| 15-19 | Cleanup when a feature PR closes | |
| 20-29 | Checks on PRs into `main` | `25_release_security_checks.yml` |
| 30-39 | Deployment and post-deployment steps on `main` | `35_refresh_bi_extracts.yml` |
| 90-99 | Scheduled or independent jobs | `99_build_custom_image.yml` |

For checks on PRs, add the [branch validation](branching-and-validation.md#branch-validation) job first and make the other jobs depend on it, as the existing stages do.

## Related

- [Use a simpler flow: feature to main](simpler-flow.md)
- [Tune PR database cleanup](pr-database-cleanup.md#tune-pr-database-cleanup)
- [Optional extensions](optional-extensions.md)
