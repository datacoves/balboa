#! /bin/bash

# Cause script to exit on error
set -e

cd "$DATACOVES__DBT_HOME"

# Make sure we have all existing tags before determining today's version
git fetch --tags

# Create a deployment version in the format YYYYMMDD.N.0
#
# The date + sequence (YYYYMMDD.N) is the human-facing deployment identifier.
# The trailing .0 keeps the string a valid 3-part semantic version, which dbt
# requires for the `version:` key in dbt_project.yml (validation is stricter in
# dbt 1.10+, Fusion and dbt 2.0). The value is only informational for dbt; the
# git tag below is the real source of truth for the deployment.
#
# Example:
#   First deployment on August 1, 2026  -> 20260801.1.0
#   Second deployment on August 1, 2026 -> 20260801.2.0
date_version=$(date -u +%Y%m%d)

# Find the highest sequence number (the N in YYYYMMDD.N.0) already used today.
last_sequence=$(
  git tag --list "${date_version}.*" |
  sed -E "s/^${date_version}\.([0-9]+)\..*/\1/" |
  grep -E '^[0-9]+$' |
  sort -n |
  tail -1
)

if [ -z "$last_sequence" ]; then
  next_sequence=1
else
  next_sequence=$((last_sequence + 1))
fi

new_version="${date_version}.${next_sequence}.0"

echo "NEW_VERSION=${new_version}" >> "$GITHUB_ENV"

# Replace the project version inside dbt_project.yml. The `^version:` anchor only
# matches the project version on its own line, never `config-version:` or
# `require-dbt-version:`.
sed -i "s/^version:.*/version: '${new_version}'/g" dbt_project.yml

# Commit and tag with [skip ci] to prevent infinite looping triggers
# https://docs.github.com/en/actions/managing-workflow-runs/skipping-workflow-runs
git add dbt_project.yml
git commit -m "Set deployment version to ${new_version} [skip ci]"
git tag -m "[skip ci]" "${new_version}"
