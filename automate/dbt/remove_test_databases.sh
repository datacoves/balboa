#!/bin/bash
set -e

export merged_prs=$(git log --all --oneline | grep -m 20 -Eo "(#([0-9]+))" | awk -F# '{print $2}' | sort -uVr | tr '\n' '|'  | sed 's/.$//')
# git log -> return commit messages from all branches
# grep: Filter to the last 20 commit messages containing "(#number)"
# awk: Trim to just the number
# sort: Cut to just unique rows, and sort by number in reverse (2 goes before 10, would be after if sorted alphabetically)
# tr, sed: Change newlines to pipes, to make a single string - and remove the last pipe

dbt run-operation remove_closed_pr_dbs --args "{pr_ids: $merged_prs}"
