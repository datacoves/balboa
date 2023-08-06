#! /bin/bash

# Cause script to exit on error
set -e

cd $DATACOVES__DBT_HOME

# Grepping and bumping version string
version_line=$(grep ^version < dbt_project.yml)
version=$(echo "${version_line}" | cut -d"'" -f2)
bumped_version=$(echo "${version}" | awk -F. '{print $1+1"."$2"."$3}')

# This is used to replace version inside project.yml
echo "NEW_VERSION=${bumped_version}" >> $GITHUB_ENV
sed -i "s/^version:.*/version: '$bumped_version'/g" dbt_project.yml

# Comminting tag with [skip ci] string to prevent infinite looping triggers
# https://docs.github.com/en/actions/managing-workflow-runs/skipping-workflow-runs
git add dbt_project.yml
git commit -am "Bumped version through github actions' [skip ci]"
git tag -m "[skip ci]" "${bumped_version}"
