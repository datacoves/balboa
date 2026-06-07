#!/usr/bin/env python3
"""
Branch validator for GitHub Actions pipelines.

Validates:
1. Source branch naming conventions (must start with 'feature', 'release',
   'hotfix', or 'infra')
2. Merge direction rules (feature->release, release->main, hotfix->main,
   infra->main)
3. Source branch is up to date with main (no commits behind)

Required environment variables (set in the GitHub Actions workflow):
  SOURCE_BRANCH      - The source (head) branch of the PR
  TARGET_BRANCH      - The target (base) branch of the PR
  GITHUB_TOKEN       - Token used to call the GitHub API
  GITHUB_REPOSITORY  - The 'owner/repo' slug (set automatically by Actions)
"""

import os
import sys

import requests


class ValidationError(Exception):
    pass


# Allowed merge targets per source branch prefix
BRANCH_RULES = {
    "feature": ("feature", "release"),
    "release": ("main",),
    "hotfix": ("main",),
    "infra": ("main",),
}

# Sort prefixes longest-first to avoid substring collisions
SORTED_PREFIXES = sorted(BRANCH_RULES.keys(), key=len, reverse=True)

VALID_SOURCE_PREFIXES = tuple(SORTED_PREFIXES)
VALID_TARGET_PREFIXES = ("feature", "release", "main")


def _get_branch_prefix(branch):
    """Return the matching prefix for a branch, or None."""
    lower = branch.lower()
    for prefix in SORTED_PREFIXES:
        if lower.startswith(prefix):
            return prefix
    return None


def validate_merge_direction(source_branch, target_branch):
    """Validate that the source branch is allowed to merge into the target."""
    prefix = _get_branch_prefix(source_branch)
    if prefix is None:
        return

    allowed = BRANCH_RULES[prefix]
    target_matches = any(
        target_branch.lower().startswith(t) for t in allowed
    )
    if not target_matches:
        allowed_str = ", ".join(allowed)
        raise ValidationError(
            f"❌ '{prefix}' branch can only be merged to: {allowed_str}"
        )


def check_branch_up_to_date(source_branch, github_token, repository):
    """
    Use the GitHub Compare API to check if the source branch is up to date
    with main. Always checks against main regardless of the PR target,
    ensuring conflicts are resolved by the feature developer before entering
    the release branch.
    Raises ValidationError if the source is behind main.
    """
    url = f"https://api.github.com/repos/{repository}/compare/{source_branch}...main"
    headers = {
        "Authorization": f"token {github_token}",
        "Accept": "application/vnd.github.v3+json",
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.RequestException as e:
        raise ValidationError(f"❌ Failed to compare branches via GitHub API: {e}")
    except ValueError as e:
        raise ValidationError(f"❌ Failed to parse GitHub API response: {e}")

    # ahead_by tells us how many commits main is ahead of source
    # (i.e., how many commits source is behind main)
    commits_behind = data.get("ahead_by", 0)

    if commits_behind == 0:
        print(f"✅ The {source_branch} is up to date with the main branch.")
    else:
        raise ValidationError(
            f"❌ There are {commits_behind} commit(s) in the main branch that are "
            f"not in source branch: {source_branch}. Pull main into the {source_branch}"
        )


def main():
    """
    Validates branch naming and merge direction rules.

    Adds source branch naming validation and ensures the correct merge
    direction (feature->release, release->main, hotfix->main, infra->main).
    """
    github_token = os.environ.get("GITHUB_TOKEN", "")
    repository = os.environ.get("GITHUB_REPOSITORY", "")

    source_branch = os.environ.get("SOURCE_BRANCH", "")
    target_branch = os.environ.get("TARGET_BRANCH", "")

    # GitHub may prefix with refs/heads/
    source_branch = source_branch.replace("refs/heads/", "")
    target_branch = target_branch.replace("refs/heads/", "")

    print(f"Source Branch: {source_branch}")
    print(f"Target Branch: {target_branch}")

    if not source_branch or not target_branch:
        raise ValidationError(
            "❌ SOURCE_BRANCH and TARGET_BRANCH environment variables must be set."
        )

    if not github_token or not repository:
        raise ValidationError(
            "❌ GITHUB_TOKEN and GITHUB_REPOSITORY environment variables must be set."
        )

    if source_branch == target_branch:
        print("Source and target branches are the same, skipping validation.")
        return

    print(
        f"Validating source branch: {source_branch} "
        f"with target branch: {target_branch}"
    )

    # Validate source branch naming
    if not any(source_branch.lower().startswith(p) for p in VALID_SOURCE_PREFIXES):
        raise ValidationError(
            f"❌ Source branch must start with one of: {', '.join(VALID_SOURCE_PREFIXES)}"
        )

    if not any(target_branch.lower().startswith(p) for p in VALID_TARGET_PREFIXES):
        raise ValidationError(
            f"❌ Target branch must start with one of: {', '.join(VALID_TARGET_PREFIXES)}"
        )

    # Validate merge direction
    validate_merge_direction(source_branch, target_branch)

    # Check if source branch is up to date with main
    check_branch_up_to_date(source_branch, github_token, repository)

    print("✅ Branch validated!")


if __name__ == "__main__":
    try:
        main()
    except ValidationError as ex:
        print(ex)
        sys.exit(1)
