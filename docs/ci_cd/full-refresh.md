# Full refresh

> Part of the [CI/CD pipelines](../ci_cd_pipelines.md) documentation.

This page covers the `full-refresh` label across [stage 10](stage-10-feature-checks.md), [stage 20](stage-20-release-checks.md) and [stage 30](stage-30-deploy-to-production.md).

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
| Release PR into `main` ([stage 20](stage-20-release-checks.md)) | Full refresh in the QA database, plus the `[deploy:full-refresh]` marker in the PR title. Stage 20 does react to the label, so adding it starts a run |
| Merge into `main` ([stage 30](stage-30-deploy-to-production.md)) | The production deployment runs `--full-refresh` when the merge commit contains `[deploy:full-refresh]`. See [Why a deploy marker in the PR title](stage-20-release-checks.md#why-a-deploy-marker-in-the-pr-title) |

To refresh only some models instead of the whole selection, see [Commit markers instead of labels](optional-extensions.md#commit-markers-instead-of-labels).
