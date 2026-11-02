#!/usr/bin/env python3
"""
Get the IDs of the feature PRs that were merged into the release branch just deployed.

Used by workflow 30 (deploy to production) targeted cleanup to decide which
PR-scoped databases to drop after a successful production deploy:
  1. Look up the PR whose merge commit is GITHUB_SHA (the commit pushed to
     main that triggered the deploy).
  2. If that PR's source branch is a release branch (release/*), list every
     completed (merged) PR from a feature branch (feature/*) into it.
  3. Print those IDs as the GitHub Actions output variable "feature_pr_ids".

The release branch is found through the GitHub API (the "list pull requests
associated with a commit" endpoint), not by parsing the commit message, so it
works with regular merges, squash merges and custom commit messages.

Deploys that don't come from a release branch (hotfix/*, infra/*) and feature
branches that don't follow the feature/* convention are skipped here; the
sweep step (sweep_stale_pr_databases.py) cleans those up.

The script never fails the step: on any error it prints a warning and an
empty output variable, so the drop step is skipped and the next deploy
retries.

Required environment variables:
  GITHUB_TOKEN      - Token used to call the GitHub API
  GITHUB_REPOSITORY - The 'owner/repo' slug
  GITHUB_SHA        - The commit that triggered the build (merge commit on main)
  GITHUB_OUTPUT     - Path to the file GitHub Actions reads step outputs from
"""

import os
import urllib.parse

import requests

# Always written when there is nothing to drop, so the variable exists (empty)
# and the drop step's condition skips it
EMPTY_OUTPUT = "feature_pr_ids="


def get_env(name):
    value = os.environ.get(name, "").strip()
    if not value:
        raise ValueError(f"{name} is not set")
    return value


def make_headers(token):
    return {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github+json",
    }


def write_output(name, value):
    with open(os.environ["GITHUB_OUTPUT"], "a") as f:
        f.write(f"{name}={value}\n")


def get_release_branch_from_api(repo, headers, commit_sha):
    """Find the PR that produced this merge commit and return its source branch.

    Uses the "list pull requests associated with a commit" endpoint. Returns
    the head branch name of the PR whose base is main, or None if not found.
    """
    url = f"https://api.github.com/repos/{repo}/commits/{commit_sha}/pulls"

    response = requests.get(url, headers=headers)
    response.raise_for_status()
    prs = response.json()

    for pr in prs:
        if pr.get("base", {}).get("ref") == "main":
            head_ref = pr.get("head", {}).get("ref", "")
            print(f"PR found: #{pr.get('number')} (source: {head_ref})")
            return head_ref

    return None


def get_feature_pr_ids(repo, headers, release_branch):
    """Return the IDs of all merged PRs from feature/* into the release branch.

    Results come in pages of up to 100 PRs; a page with fewer than 100
    (counting all PRs targeting the release branch, not only feature/*) is
    the last one.
    """
    pr_ids = []
    page = 1

    while True:
        params = {
            "state": "closed",
            "base": release_branch,
            "per_page": "100",
            "page": str(page),
        }
        url = f"https://api.github.com/repos/{repo}/pulls?{urllib.parse.urlencode(params)}"

        response = requests.get(url, headers=headers)
        response.raise_for_status()
        batch = response.json()

        for pr in batch:
            head_ref = pr.get("head", {}).get("ref", "")
            if pr.get("merged_at") and head_ref.startswith("feature/"):
                pr_ids.append(str(pr["number"]))

        if len(batch) < 100:
            break
        page += 1

    return pr_ids


def main():
    commit_sha = get_env("GITHUB_SHA")
    print(f"Source commit: {commit_sha}")

    repo = get_env("GITHUB_REPOSITORY")
    token = get_env("GITHUB_TOKEN")
    headers = make_headers(token)

    release_branch = get_release_branch_from_api(repo, headers, commit_sha)

    if not release_branch:
        print("No PR found for source commit. Skipping cleanup.")
        write_output("feature_pr_ids", "")
        return

    if not release_branch.startswith("release/"):
        print(f"Source branch is '{release_branch}', not a release branch. Skipping cleanup.")
        write_output("feature_pr_ids", "")
        return

    print(f"Release branch: {release_branch}")

    pr_ids = get_feature_pr_ids(repo, headers, release_branch)

    if pr_ids:
        ids_str = ",".join(pr_ids)
        print(f"Feature PR IDs to clean up: {ids_str}")
        write_output("feature_pr_ids", ids_str)
    else:
        print("No feature PRs found for this release branch.")
        write_output("feature_pr_ids", "")


def run():
    # Never exit non-zero: cleanup must not affect the deploy. An empty output
    # variable makes the drop step skip; the next deploy (or the sweep) retries.
    try:
        main()
    except requests.exceptions.RequestException as e:
        print(f"WARNING: GitHub API call failed: {e}")
        write_output("feature_pr_ids", "")
    except Exception as e:
        print(f"WARNING: Failed to get feature PR IDs: {e}")
        write_output("feature_pr_ids", "")


if __name__ == "__main__":
    run()
