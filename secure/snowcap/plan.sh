#!/bin/bash
secure_path=/config/workspace/secure/snowcap
cd $secure_path

if [ -f .env ]; then
    echo "File .env found."
else
    echo "File .env does not exist. Please create a .env file with the following variables:"
    echo ""
    echo "SNOWFLAKE_ACCOUNT="
    echo "SNOWFLAKE_ACCOUNT_PII="
    echo "SNOWFLAKE_USER="
    echo "SNOWFLAKE_ROLE="
    echo "SNOWFLAKE_PRIVATE_KEY_PATH="
    echo "SNOWFLAKE_AUTHENTICATOR=SNOWFLAKE_JWT"
    echo ""
    exit 1
fi

# Load env vars safely
set -a
. ./.env
set +a

# Default to non-PII (standard) account
ACCOUNT_TO_USE="$SNOWFLAKE_ACCOUNT"
EXCLUDE_RESOURCES=""

# If -pii flag is passed, switch to PII (enterprise) account
if [[ "$1" == "-pii" ]]; then
  ACCOUNT_TO_USE="$SNOWFLAKE_ACCOUNT_PII"
else
  # Standard accounts don't support enterprise-only features
  EXCLUDE_RESOURCES="--exclude masking_policy,tag,tag_reference,tag_masking_policy_reference"
fi

# Override SNOWFLAKE_ACCOUNT for the snowcap run
export SNOWFLAKE_ACCOUNT="$ACCOUNT_TO_USE"

echo "=========="
echo "Using SNOWFLAKE_ACCOUNT=$SNOWFLAKE_ACCOUNT"
if [[ -n "$EXCLUDE_RESOURCES" ]]; then
  echo "Excluding enterprise-only resources (standard account)"
fi
echo "=========="

# uvx --from snowcap@git+https://github.com/datacoves/snowcap.git@improve_plan_output \
#     --refresh \
#     snowcap plan \
#     --config resources/ \
#     --sync_resources role,grant,role_grant,warehouse,user,masking_policy,tag,tag_reference,tag_masking_policy_reference \
#     $EXCLUDE_RESOURCES

uvx snowcap plan \
    --config resources/ \
    --sync_resources role,grant,role_grant,warehouse,user,masking_policy,tag,tag_reference,tag_masking_policy_reference \
    $EXCLUDE_RESOURCES
