#!/usr/bin/env python3

import os
import sys
import subprocess

class ValidationError(Exception):
    pass

class GitCommandError(Exception):
    pass

def get_commit_count(branch):
    """
    Get the count of commits that are in the current branch (HEAD) but not in the main branch origin/main.
    """
    try:
        result = subprocess.run([
                "git",
                "rev-list",
                "--count",
                "HEAD..origin/main"],
            capture_output=True,
            text=True,
            check=True)
        return int(result.stdout.strip())
    except subprocess.CalledProcessError as e:
        raise GitCommandError(f"Failed to get commit count between {branch} and main: {e}")

def main():
    """
    Runs some validations on branches given that CHANGE_BRANCH (source branch) and
    CHANGE_TARGET (target branch) are set as environment vars before running this.

        Raises:
           Exception: Validations not passed
    """

    # Retrieve branch names from environment variables
    source_branch = os.environ.get("SOURCE_BRANCH")
    target_branch = os.environ.get("TARGET_BRANCH")
    print(f"Source Branch: {source_branch}")
    print(f"Target Branch: {target_branch}")

    if not source_branch or not target_branch:
        print("ERROR: SOURCE_BRANCH and TARGET_BRANCH environment variables must be set.")
        sys.exit(1)

    # Get the commit count for changes in main that are not in source_branch
    try:
        commits_behind = get_commit_count(source_branch)
    except GitCommandError as e:
        print(e)
        sys.exit(1)

    if commits_behind > 0:
        raise ValidationError(f"There are commits in main that are not in Source branch: {source_branch}. Pull main branch into {source_branch}.")

    if source_branch != target_branch:
        print(
            "Validating source branch: "
            + source_branch
            + " with target branch: "
            + target_branch
        )
        if not source_branch.lower().startswith(("feature", "release")):
            raise ValidationError(
                "Source branch must start with 'feature' or 'release'"
            )

        if not target_branch.lower().startswith(("feature", "release", "main")):
            raise ValidationError(
                "Target branch must start with 'feature', 'release' or 'main'"
            )

        # check the correct order
        if source_branch.lower().startswith(("feature")):
            if not target_branch.lower().startswith(("feature", "release")):
                raise ValidationError(
                    "Feature branch can only be merged to another feature branch or a release branch"
                )

        if source_branch.lower().startswith(("release")):
            if not target_branch.lower().startswith(("main")):
                raise ValidationError(
                    "Release branch can only be merged to the main branch"
                )

    print("Branch names validated!")

if __name__ == "__main__":
    try:
        main()
    except Exception as ex:
        print(ex)
        exit(1)
