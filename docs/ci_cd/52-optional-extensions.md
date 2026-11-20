# Optional extensions

> Part of the [CI/CD pipelines](../ci_cd_pipelines.md) documentation.

If your team needs more than the default pipelines, these are some ways to extend them.

| Extension | Stages it changes |
|---|---|
| [Commit markers instead of labels](#commit-markers-instead-of-labels) | 10, 20 and 30 |
| [Wider selection with @state:modified](#wider-selection-with-statemodified) | 10, 20 and 30 |
| [Enforce a branch name format](#enforce-a-branch-name-format) | Branch validation (10 and 20) |
| [Block dbt changes from infra branches](#block-dbt-changes-from-infra-branches) | 20 |

## Commit markers instead of labels

Instead of adding the `full-refresh` label to a PR, developers write markers in their commit messages:

| Marker in the commit message | What the workflow does |
|---|---|
| `[full-refresh]` | Full-refresh build of the whole selection, like the [`full-refresh` label](40-full-refresh.md) |
| `[full-refresh: model_a, model_b]` | Full refresh of only the listed models. The script skips the models that do not exist in production yet, because they are built from scratch anyway |
| `[fresh-db]` | Drops and recreates the PR database before building (stage 10 only). The script reads this marker only from the commits pushed since the previous run, so it fires once instead of on every push |

Each workflow needs a script that parses the markers from the commits and sets them as step outputs, so the `--full-refresh` steps can be gated on them:

- **Stage 10** parses them from the commits of the feature PR and applies the full or targeted refresh, and `[fresh-db]`, to the PR database.
- **Stage 20** parses them from the commits of the PR into `main` and applies the same full or targeted refresh to the QA build.
- **Stage 30** reads them from every commit since the last deployment tag. It refreshes the targeted models and excludes them from the rest of the blue/green build. Do the targeted refresh in the staging database that the blue/green deployment builds, not in production: refreshing production before the blue/green run changes it before the swap, so it is no longer a zero-downtime deployment, and a failed blue/green run leaves production half updated.

Whether the markers reach stage 30 depends on how PRs are merged:

- **Create a merge commit** or **Rebase and merge** keep every commit, so the markers reach `main`.
- **Squash and merge** replaces the commits with one commit, whose message depends on the repository's default squash commit message (**Settings › General › Pull Requests**):
  - With the GitHub default, the message includes the messages of the squashed commits, so the markers survive.
  - With **Pull request title and description**, they survive only if they are also in the PR description. Have developers copy them there, or have stage 10 or 20 append them to the description.
  - With **Pull request title**, they are lost.

## Wider selection with @state:modified

Select `@state:modified` instead of `state:modified+`. The `@` operator builds the changed models, their descendants, **and every ancestor of those descendants**. Relationship tests then compare tables that were built in the same run, instead of comparing a freshly built child against an older production parent. The cost is a larger build.

Change the selection in:

- **Stage 10:** the slim-mode commands of step 8 (governance build with empty models) and step 11 (`dbt build`).
- **Stage 20:** the slim-mode command of step 6 (`dbt build`).
- **Stage 30:** the *Run blue / green deployment with defer* step. Replace `--defer` with the selector:

  ```bash
  dbt-coves blue-green --dbt-selector="--defer --state logs -s @state:modified"
  ```

  Don't keep `--defer` next to `--dbt-selector`: when `--defer` is set, `dbt-coves blue-green` ignores `--dbt-selector` and builds `state:modified+`. Leave the *Run blue / green deployment* step, which runs when no production manifest is found, as it is.

## Enforce a branch name format

The validator only checks prefixes, so a branch named `featurex` passes as `feature`. Some teams also want every branch name to carry a ticket, a date and an owner. To do that, validate the **full branch name** with one regular expression per prefix, plus a human-readable format and an example that the error message shows. For example, with short prefixes:

```python
import re

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

Check the format of the source branch, and of the target branch when it is not `main`. Also check that the `yyyymmdd` part is a valid date. To switch to short prefixes like these, see [Change branch prefixes](50-customization.md#change-branch-prefixes).

## Block dbt changes from infra branches

Infra branches go straight to `main` and are meant for workflow and tooling changes. To make sure that no dbt model change skips the release process, add a step to the `validate-branch` job of stage 20 that fails the PR when an infra branch changes anything under `transform/`:

```yaml
- name: Block transform changes from infra branches
  if: startsWith(github.head_ref, 'infra/')
  run: |
    CHANGED=$(git diff --name-only "origin/$TARGET_BRANCH...HEAD" -- transform/ ':(exclude)transform/macros/tooling/')
    if [ -n "$CHANGED" ]; then
      echo "ERROR: infra branches must not modify the transform/ folder (except transform/macros/tooling/):"
      echo "$CHANGED"
      exit 1
    fi
```

The step needs the history of the target branch, which the job's checkout does not fetch by default. Add `fetch-depth: 0` to it:

```yaml
- name: Checkout code
  uses: actions/checkout@v5
  with:
    fetch-depth: 0
```

The check excludes `transform/macros/tooling/`, which holds the dbt macros that the workflows themselves use, such as database creation, clone, swap and PR database cleanup. That way an infra PR can change a workflow and the macro it calls together. Every other change under `transform/`, such as models, other macros or `dbt_project.yml`, still has to go through a feature branch and a release.

Stage 20 only triggers on changes under `transform/`, so it runs on every infra PR that needs this check. Infra PRs that only change `transform/macros/tooling/` also trigger it, and pass.
