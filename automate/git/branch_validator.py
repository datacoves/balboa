#!/usr/bin/env python3

import os
import sys
import requests

class ValidationError(Exception):
    pass

class GitCommandError(Exception):
    pass

def get_commit_count(source_branch, github_token, repository):
    """
    Use GitHub API to compare branches and get commit count difference.
    This tells us how many commits behind target the source branch is.
    """
    try:
        # GitHub API endpoint for comparing branches
        url = f"https://api.github.com/repos/{repository}/compare/{source_branch}...main"

        headers = {
            "Authorization": f"token {github_token}",
            "Accept": "application/vnd.github.v3+json"
        }

        response = requests.get(url, headers=headers)
        response.raise_for_status()

        data = response.json()

        # ahead_by tells us how many commits target is ahead of source
        # (i.e., how many commits source is behind target)
        commits_behind = data.get("ahead_by", 0)

        if commits_behind == 0:
            print(f"✅ The {source_branch} is up to date with the main branch.")
        else:
            print(f"❌ There are {commits_behind} commit(s) in the main branch that are not in Source branch: {source_branch}. Pull main into the {source_branch}")

    except requests.exceptions.RequestException as e:
        raise GitCommandError(f"❌ Failed to compare branches via GitHub API: {e}")
    except (KeyError, ValueError) as e:
        raise GitCommandError(f"❌ Failed to parse GitHub API response: {e}")

def main():
    """
    Runs some validations on branches given that SOURCE_BRANCH and
    TARGET_BRANCH are set as environment vars before running this.

        Raises:
           Exception: Validations not passed
    """
    # Get GitHub API credentials
    github_token = os.environ.get("GITHUB_TOKEN")
    repository = os.environ.get("GITHUB_REPOSITORY")

    # Retrieve branch names from environment variables
    source_branch = os.environ.get("SOURCE_BRANCH")
    target_branch = os.environ.get("TARGET_BRANCH")
    print(f"Source Branch: {source_branch}")
    print(f"Target Branch: {target_branch}")

    if not source_branch or not target_branch:
        print("❌ ERROR: SOURCE_BRANCH and TARGET_BRANCH environment variables must be set.")
        sys.exit(1)

    if not github_token or not repository:
        print("❌ ERROR: GITHUB_TOKEN and GITHUB_REPOSITORY environment variables must be set.")
        sys.exit(1)

    # Get the commit count for changes in target that are not in source_branch
    try:
        get_commit_count(source_branch, github_token, repository)
    except GitCommandError as e:
        print(e)
        sys.exit(1)

    if source_branch != target_branch:
        print(
            "Validating source branch: "
            + source_branch
            + " with target branch: "
            + target_branch
        )
        if not source_branch.lower().startswith(("feature", "release")):
            raise ValidationError(
                "❌ Source branch must start with 'feature' or 'release'"
            )

        if not target_branch.lower().startswith(("feature", "release", "main")):
            raise ValidationError(
                "❌ Target branch must start with 'feature', 'release' or 'main'"
            )

        # check the correct order
        if source_branch.lower().startswith(("feature")):
            if not target_branch.lower().startswith(("feature", "release")):
                raise ValidationError(
                    "❌ Feature branch can only be merged to another feature branch or a release branch"
                )

        if source_branch.lower().startswith(("release")):
            if not target_branch.lower().startswith(("main")):
                raise ValidationError(
                    "❌ Release branch can only be merged to the main branch"
                )

    print("✅ Branch validated!")

if __name__ == "__main__":
    try:
        main()
    except Exception as ex:
        print(ex)
        exit(1)
