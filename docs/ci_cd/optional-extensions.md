# Optional extensions

> Part of the [CI/CD pipelines](../ci_cd_pipelines.md) documentation.

These extensions are not implemented in this repository. They come from teams that needed them.

## Commit markers instead of labels

Instead of the `full-refresh` PR label, developers write markers in their commit messages:

| Marker | Effect |
|---|---|
| `[full-refresh]` | Full-refresh build of the whole project |
| `[full-refresh: model_a, model_b]` | Full refresh of only the listed models. Skip the models that do not exist in production yet, because they are built from scratch anyway |
| `[fresh-db]` | Drops and recreates the PR database before building. Read it only from the commits pushed since the previous run, so it fires once instead of on every push |

The workflows need a script that parses the markers from the PR's commits and sets them as step outputs, so the `--full-refresh` steps can be gated on them. Also copy the markers into the PR description: that way they survive a squash merge.

- **Stage 20** parses them from the commits of the release PR and applies the same full or targeted refresh to the QA build.
- **Stage 30** reads them from every commit since the last deployment tag. It refreshes the targeted models before the blue/green run and excludes them from it.

## Wider selection with @state:modified

Select `@state:modified` instead of `state:modified+`. The `@` operator builds the changed models, their descendants, **and every parent of those descendants**. Relationship tests then compare tables that were built in the same run, instead of comparing a freshly built child against an older production parent. The cost is a larger build.

In stage 30, pass it to the blue/green build with `dbt-coves blue-green --dbt-selector="--defer --state logs -s @state:modified"`.

## Enforce a branch name format

The validator only checks prefixes. Some teams also want every branch name to carry a ticket, a date and an owner. To do that, validate the **full branch name** with one regular expression per prefix, plus a human-readable format and an example that the error message shows. For example, with short prefixes:

```python
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

Check the format on both the source and the target branch, and also check that the `yyyymmdd` part is a valid date. To switch to short prefixes like these, see [Change branch prefixes](customization.md#change-branch-prefixes).

## Block dbt changes from infra branches

Infra branches go straight to `main` and are meant for workflow and tooling changes. To make sure that no dbt model change skips the release process, add a step to the validation job of stage 20 that fails the PR when an infra branch changes anything under `transform/`:

```bash
CHANGED=$(git diff --name-only origin/$TARGET_BRANCH HEAD -- transform/ ':(exclude)transform/macros/tooling/')
if [ -n "$CHANGED" ]; then
  echo "ERROR: infra branches must not modify the transform/ folder (except transform/macros/tooling/)."
  exit 1
fi
```

The check excludes `transform/macros/tooling/`, which holds the dbt macros that the workflows themselves use, such as database creation, clone, swap and PR database cleanup. That way an infra PR can change a workflow and the macro it calls together. Every other change under `transform/`, such as models, other macros or `dbt_project.yml`, still has to go through a feature branch and a release.

Run this step only when `github.head_ref` starts with `infra/`. Because stage 20 only triggers on changes under `transform/`, it runs on exactly the infra PRs this step has to check.
