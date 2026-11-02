#!/usr/bin/env python3
"""
Sweep cleanup for orphaned TST (PR-scoped) databases.

Used by workflow 30 after the targeted cleanup (get_feature_pr_ids.py). The
workflow lists all "<base>_PR_*" databases in Snowflake (show_tst_databases
macro) and passes them in TST_DATABASE_LIST. This script checks each
database's PR in GitHub and prints the IDs that can be dropped as the
GitHub Actions output variable "sweep_pr_ids". It does not drop anything
itself.

It catches what the targeted cleanup misses: hotfix/infra deploys that
bypass the release branch, abandoned releases, feature branches outside the
feature/* convention, and cleanups that failed on a previous deploy.

Decision per database, based on its PR:
  - PR abandoned (closed, not merged)   -> drop
  - PR not found (404, deleted)         -> drop
  - PR open                             -> keep (still in development)
  - PR merged into a non-release branch -> drop (e.g. merged directly to main)
  - PR merged into release/*            -> depends on the release, rules below
  - Any other API error                 -> keep for now, retried on the next deploy

Release rules, evaluated in this order (the first match wins):
  1. Release has an open PR to main              -> keep (release in QA)
  2. Release has a merged PR to main              -> drop (release shipped)
  3. Release has an abandoned PR to main          -> drop (release abandoned)
  4. Release branch still exists, no PR to main   -> keep (QA before the release
                                                     PR is opened), unless the TST
                                                     database is older than
                                                     TST_MAX_AGE_DAYS and idle
                                                     (see below)
  5. Release branch deleted, no PR to main        -> drop (release discarded)

Rule 1 goes first because a release PR can be abandoned and reopened: if any
PR to main is open, the release is still in progress. Rule 4 has an age limit
because release branches are not deleted after merging, so a release that
never gets a PR to main would otherwise keep its TST databases forever. Age
is measured from the database's created_on in Snowflake.

Rule 4 age limit in detail:
  - age <= TST_MAX_AGE_DAYS                           -> keep
  - age >  TST_MAX_AGE_DAYS, queried in the last
    TST_MAX_IDLE_DAYS (by anyone but the CI user)      -> keep (still in use)
  - age >  TST_MAX_AGE_DAYS, no recent queries         -> drop
  - age >  TST_HARD_MAX_AGE_DAYS                       -> drop, even if in use
The hard limit keeps every TST's lifetime bounded. Activity comes from the
last_query field (ACCOUNT_USAGE.QUERY_HISTORY, see show_tst_databases macro).

Required environment variables:
  GITHUB_TOKEN          - Token used to call the GitHub API
  GITHUB_REPOSITORY     - The 'owner/repo' slug
  GITHUB_OUTPUT         - Path to the file GitHub Actions reads step outputs from
  TST_DATABASE_LIST     - Pipe-separated list of "<name>,<created_on ISO>,<last_query ISO>"
                          entries for "<base>_PR_*" databases (provided by the
                          workflow step that runs the show_tst_databases macro);
                          last_query is empty when there was no recent activity
  TST_MAX_AGE_DAYS      - Optional, default 15. Max age in days for rule 4.
  TST_MAX_IDLE_DAYS     - Optional, default 15. Days without queries after which
                          a rule-4 TST older than TST_MAX_AGE_DAYS is dropped.
  TST_HARD_MAX_AGE_DAYS - Optional, default 45. Rule-4 TSTs older than this are
                          dropped regardless of activity.
  The day settings must be integers; an invalid value aborts the sweep (nothing
  is dropped).
"""

import os
import re
import urllib.parse

import requests
from datetime import datetime, timezone

EMPTY_OUTPUT = "sweep_pr_ids="
DEFAULT_MAX_AGE_DAYS = 15
DEFAULT_MAX_IDLE_DAYS = 15
DEFAULT_HARD_MAX_AGE_DAYS = 45
# Reason returned by get_release_decision for rule 4; main() uses it to apply the age limit
RULE_NO_PR_YET = "branch exists, no PR to main yet"


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


def get_pr_details(repo, headers, pr_number):
    """Get the state, merged flag and target branch of a single PR.

    Raises requests.exceptions.HTTPError (404 if the PR was deleted); main()
    handles it.
    """
    url = f"https://api.github.com/repos/{repo}/pulls/{pr_number}"
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()
    state = data.get("state", "unknown")
    merged = bool(data.get("merged"))
    target_branch = data.get("base", {}).get("ref", "")
    return state, merged, target_branch


def get_pr_statuses_to_main(repo, headers, source_branch):
    """Return the set of statuses ("open", "merged", "abandoned") of all PRs
    from a branch to main.

    Uses state=all because a release can have several PRs to main over time
    (e.g. abandoned and reopened); the rules need to see all of them. GitHub's
    `head` filter requires the "owner:branch" form, not a bare branch name.
    """
    owner = repo.split("/")[0]
    params = {
        "state": "all",
        "head": f"{owner}:{source_branch}",
        "base": "main",
        "per_page": "100",
    }
    url = f"https://api.github.com/repos/{repo}/pulls?{urllib.parse.urlencode(params)}"

    response = requests.get(url, headers=headers)
    response.raise_for_status()
    prs = response.json()

    statuses = set()
    for pr in prs:
        if pr.get("state") == "open":
            statuses.add("open")
        elif pr.get("merged_at"):
            statuses.add("merged")
        else:
            statuses.add("abandoned")
    return statuses


def branch_exists(repo, headers, branch):
    """Check if a branch still exists in the repository."""
    quoted_branch = urllib.parse.quote(branch, safe="")
    url = f"https://api.github.com/repos/{repo}/branches/{quoted_branch}"
    response = requests.get(url, headers=headers)
    if response.status_code == 404:
        return False
    response.raise_for_status()
    return True


def get_release_decision(repo, headers, release_branch):
    """Decide whether TST databases for a release branch can be dropped.

    Returns a (droppable, reason) tuple; the reason is printed in the log.
    Implements release rules 1-5 from the module docstring. The age limit of
    rule 4 is applied by main(), because it depends on each database.
    """
    statuses = get_pr_statuses_to_main(repo, headers, release_branch)

    if "open" in statuses:
        return False, "has open PR to main"
    if "merged" in statuses:
        return True, "release PR to main merged"
    if "abandoned" in statuses:
        return True, "release PR to main abandoned"
    if branch_exists(repo, headers, release_branch):
        return False, RULE_NO_PR_YET
    return True, "branch deleted, no PR to main"


def get_age_days(created_on):
    """Return the age in whole days of an ISO timestamp, or None if it can't be parsed.

    None means the age is unknown, and main() then keeps the database (the safe
    choice). Timestamps without a timezone are treated as UTC.
    """
    if not created_on:
        return None
    try:
        created = datetime.fromisoformat(created_on)
    except ValueError:
        return None
    if created.tzinfo is None:
        created = created.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - created).days


def parse_db_list(db_list_raw):
    """Extract PR IDs from database names ("<base>_PR_<id>"); each entry is
    "<name>,<created_on>,<last_query>" (last_query may be empty).

    Returns (pr_map, created_map, last_query_map) keyed by PR id.
    """
    entries = [entry.strip() for entry in db_list_raw.split("|") if entry.strip()]

    pr_map = {}
    created_map = {}
    last_query_map = {}
    for entry in entries:
        db_name, _, rest = entry.partition(",")
        created_on, _, last_query = rest.partition(",")
        match = re.search(r"_PR_(\d+)$", db_name, re.IGNORECASE)
        if match:
            pr_id = match.group(1)
            pr_map[pr_id] = db_name
            created_map[pr_id] = created_on.strip()
            last_query_map[pr_id] = last_query.strip()

    return pr_map, created_map, last_query_map


def sweep(repo, headers, pr_map, created_map, last_query_map, max_age_days, max_idle_days, hard_max_age_days):
    """Decide which PR ids can be dropped. Returns the list of droppable PR ids."""
    release_decisions = {}
    ids_to_drop = []

    for pr_id, db_name in pr_map.items():
        try:
            state, merged, target_branch = get_pr_details(repo, headers, pr_id)

            if state == "closed" and not merged:
                print(f"  {db_name} (PR #{pr_id}): abandoned — will drop")
                ids_to_drop.append(pr_id)
            elif merged:
                # Merged into a release branch: the TST database lives as long
                # as that release is in progress (release rules 1-5)
                if target_branch.startswith("release/"):
                    if target_branch not in release_decisions:
                        release_decisions[target_branch] = get_release_decision(
                            repo, headers, target_branch
                        )
                    droppable, reason = release_decisions[target_branch]
                    age_days = get_age_days(created_map.get(pr_id))
                    # Rule 4 age limit: only when the release has no PR to main yet
                    # and the age is known
                    if not droppable and reason == RULE_NO_PR_YET and age_days is not None and age_days > max_age_days:
                        # None = no queries in the lookback window (idle)
                        idle_days = get_age_days(last_query_map.get(pr_id))
                        if age_days > hard_max_age_days:
                            print(f"  {db_name} (PR #{pr_id}): merged, {target_branch} {reason}, "
                                  f"database is {age_days} days old (hard max {hard_max_age_days}) — will drop")
                            ids_to_drop.append(pr_id)
                        elif idle_days is not None and idle_days <= max_idle_days:
                            print(f"  {db_name} (PR #{pr_id}): merged, {target_branch} {reason}, "
                                  f"database is {age_days} days old but was queried {idle_days} days ago "
                                  f"(max idle {max_idle_days}) — keeping")
                        else:
                            idle_text = f"{idle_days} days" if idle_days is not None else "no recent queries"
                            print(f"  {db_name} (PR #{pr_id}): merged, {target_branch} {reason}, "
                                  f"database is {age_days} days old (max {max_age_days}) and idle "
                                  f"({idle_text}, max idle {max_idle_days}) — will drop")
                            ids_to_drop.append(pr_id)
                    elif droppable:
                        print(f"  {db_name} (PR #{pr_id}): merged, {target_branch} {reason} — will drop")
                        ids_to_drop.append(pr_id)
                    else:
                        print(f"  {db_name} (PR #{pr_id}): merged, {target_branch} {reason} — keeping")
                else:
                    # Merged somewhere other than a release (e.g. directly to main)
                    print(f"  {db_name} (PR #{pr_id}): merged (target: {target_branch}) — will drop")
                    ids_to_drop.append(pr_id)
            else:
                # open / draft: still in development
                print(f"  {db_name} (PR #{pr_id}): {state} — keeping")
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 404:
                print(f"  {db_name} (PR #{pr_id}): not found — will drop")
                ids_to_drop.append(pr_id)
            else:
                # Unknown state: keep it now, the next deploy retries
                status = e.response.status_code if e.response is not None else "unknown"
                print(f"  {db_name} (PR #{pr_id}): API error {status} — skipping")

    return ids_to_drop


def main():
    db_list_raw = os.environ.get("TST_DATABASE_LIST", "").strip()
    if not db_list_raw:
        print("No TST databases found in Snowflake. Nothing to sweep.")
        write_output("sweep_pr_ids", "")
        return

    pr_map, created_map, last_query_map = parse_db_list(db_list_raw)

    if not pr_map:
        print("No valid PR-scoped databases found. Nothing to sweep.")
        write_output("sweep_pr_ids", "")
        return

    print(f"Found {len(pr_map)} TST database(s) to check: {', '.join(pr_map.values())}")

    max_age_days = int(os.environ.get("TST_MAX_AGE_DAYS", "").strip() or DEFAULT_MAX_AGE_DAYS)
    max_idle_days = int(os.environ.get("TST_MAX_IDLE_DAYS", "").strip() or DEFAULT_MAX_IDLE_DAYS)
    hard_max_age_days = int(os.environ.get("TST_HARD_MAX_AGE_DAYS", "").strip() or DEFAULT_HARD_MAX_AGE_DAYS)

    repo = get_env("GITHUB_REPOSITORY")
    token = get_env("GITHUB_TOKEN")
    headers = make_headers(token)

    ids_to_drop = sweep(
        repo, headers, pr_map, created_map, last_query_map,
        max_age_days, max_idle_days, hard_max_age_days,
    )

    if ids_to_drop:
        ids_str = ",".join(ids_to_drop)
        print(f"PR IDs to sweep: {ids_str}")
        write_output("sweep_pr_ids", ids_str)
    else:
        print("No orphaned TST databases found.")
        write_output("sweep_pr_ids", "")


def run():
    # Never exit non-zero: cleanup must not affect the deploy. On an unexpected
    # error (including an invalid *_DAYS env var) nothing is dropped, and the
    # next deploy retries.
    try:
        main()
    except Exception as e:
        print(f"WARNING: Sweep cleanup failed: {e}")
        write_output("sweep_pr_ids", "")


if __name__ == "__main__":
    run()
