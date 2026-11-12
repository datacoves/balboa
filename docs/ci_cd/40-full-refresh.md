# Full refresh

> Part of the [CI/CD pipelines](../ci_cd_pipelines.md) documentation.

By default, CI does not rebuild incremental models from scratch. In a feature PR, they are cloned from production (step 7 of the [stage 10 dbt checks](10-feature-checks.md#dbt-checks)) and then built incrementally, and production builds them incrementally too. When a change requires rebuilding them, add the `full-refresh` label to the PR, and CI adds `--full-refresh` to its `dbt build` commands.

Typical reasons are a new or removed column, a changed `unique_key`, or a fix to historical logic. For example, `earthquakes_around_the_world` is incremental with `on_schema_change='fail'`, so adding a column to it fails the build unless it runs with `--full-refresh`.

## How it works in each stage

| Stage | How a full refresh is requested | What is rebuilt |
|---|---|---|
| [10](10-feature-checks.md): feature PR into a release branch | `full-refresh` label on the feature PR | The PR database only. It never reaches production |
| [20](20-release-checks.md): PR into `main` (release, hotfix or infra) | `full-refresh` label on the PR into `main`. Stage 20 also adds the `[deploy:full-refresh]` marker to the PR title | The QA database |
| [30](30-deploy-to-production.md): merge into `main` | No label: stage 30 looks for `[deploy:full-refresh]` in the merge commit message | Production |

In every stage, `--full-refresh` applies to the same selection as a normal run. When the production manifest is found, that is the changed models and their downstream models (`state:modified+`), not the whole project. Without a manifest, the whole project is rebuilt. The exception is the demo step that refreshes dynamic tables before the stage 30 deployment: it always selects every dynamic table, `stg_test_failures` and every seed, so with the marker all of them are fully refreshed in production.

`--full-refresh` does not rebuild snapshots. dbt ignores the flag for them.

## Full refresh on a feature PR (stage 10)

The stage 10 dbt workflow reads the label into a job-level variable:

```yaml
env:
  FULL_REFRESH_FLAG: ${{ contains(github.event.pull_request.labels.*.name, 'full-refresh') && '--full-refresh' || '' }}
```

It appends this variable to the governance build with empty models (step 8) and to the real build (step 11). With the label, step 11 becomes:

```bash
dbt build --fail-fast --defer --state logs --select state:modified+ --full-refresh
```

To run it:

1. **Create the label, once per repository.** In GitHub, go to **Pull requests › Labels › New label** and name it exactly `full-refresh`. A description such as *Rebuild incremental models with dbt --full-refresh* helps others know what it does.

2. **Add the label to the PR.** Open the PR and select `full-refresh` under **Labels** in the right sidebar.

3. **Push a new commit.** Adding a label does not start stage 10. If you have nothing else to change, push an empty commit from the terminal of your Datacoves environment:

   ```bash
   git commit --allow-empty -m "Trigger full refresh"
   git push
   ```

   Other ways to start a run:
   - **Re-run from the Actions tab:** doesn't work. A re-run is another attempt of the same run, for the same event, so it doesn't see a label added after that event.
   - **Run workflow (manual run):** doesn't work. A manual run has no PR number, so it cannot find the PR database.
   - **Close and reopen the PR:** works, but takes longer. Closing the PR runs [stage 15](15-drop-pr-database.md), which drops the PR database. On reopen, stage 10 creates it again, clones the incremental models from production and then rebuilds them with `--full-refresh`.

4. **Check the log.** Open the run from the **Checks** tab of the PR. The commands in the *Governance run of dbt with EMPTY models* and *Run dbt build* steps should end with `--full-refresh`.

The label stays on the PR, so every later push also runs a full refresh. Once the PR database has been rebuilt, remove the label from the **Labels** sidebar, unless later pushes need it too.

## Full refresh in QA and production (stages 20 and 30)

Add the `full-refresh` label to the PR into `main`. Unlike stage 10, stage 20 reacts to the label, so you don't need to push again:

1. **When you add the label,** stage 20 appends `[deploy:full-refresh]` to the PR title, posts a comment, and rebuilds the QA database with `--full-refresh`.
2. **When you merge,** the PR title becomes part of the merge commit message. Stage 30 finds the marker there and runs the production deployment with `--full-refresh`. This covers the blue/green build and the refresh of dynamic tables and seeds that runs before it.

If you remove the label before merging, stage 20 removes the marker from the title and production is deployed normally.

> [!WARNING]
> Before you merge, check that the PR title still ends with `[deploy:full-refresh]`. Use **Create a merge commit**, or **Squash and merge** on a PR with more than one commit. With **Rebase and merge**, or a squash of a single-commit PR with the default settings, the commit message does not include the PR title, and production is deployed without `--full-refresh`. This repository allows all three merge methods; to rule out a rebase merge, disable **Allow rebase merging** in **Settings › General › Pull Requests**. See [Why a deploy marker in the PR title](20-release-checks.md#why-a-deploy-marker-in-the-pr-title).

To confirm the deployment after the merge, open the stage 30 run in the **Actions** tab: the *Check for full-refresh marker in merge commit message* step prints whether it found the marker.

## Refresh only some models

The label refreshes the whole selection. To refresh only some models, see [Commit markers instead of labels](52-optional-extensions.md#commit-markers-instead-of-labels).
