#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# Load .env if it exists
if [ -f .env ]; then
    set -a
    . ./.env
    set +a
fi

# Parse arguments
USE_PII=false
GIT_BRANCH=""
USE_PLAN=false
while [[ $# -gt 0 ]]; do
    case $1 in
        --pii)
            USE_PII=true
            shift
            ;;
        -b|--branch)
            GIT_BRANCH="$2"
            shift 2
            ;;
        --plan)
            USE_PLAN=true
            break  # Stop parsing, rest goes to snowcap (including --plan)
            ;;
        *)
            break  # Stop parsing, rest goes to snowcap
            ;;
    esac
done

# Validate required variables
missing=()
[ -z "$SNOWFLAKE_ACCOUNT" ] && missing+=("SNOWFLAKE_ACCOUNT")
[ -z "$SNOWFLAKE_USER" ] && missing+=("SNOWFLAKE_USER")
[ -z "$SNOWFLAKE_ROLE" ] && missing+=("SNOWFLAKE_ROLE")
[ -z "$SNOWFLAKE_PRIVATE_KEY_PATH" ] && missing+=("SNOWFLAKE_PRIVATE_KEY_PATH")

if $USE_PII && [ -z "$SNOWFLAKE_ACCOUNT_PII" ]; then
    missing+=("SNOWFLAKE_ACCOUNT_PII")
fi

if [ ${#missing[@]} -gt 0 ]; then
    echo "Error: Missing required environment variables:"
    for var in "${missing[@]}"; do
        echo "  - $var"
    done
    echo ""
    echo "Create a .env file with:"
    echo ""
    echo "  SNOWFLAKE_ACCOUNT=your_account        # Standard account identifier"
    echo "  SNOWFLAKE_ACCOUNT_PII=your_pii_acct   # Enterprise account (for --pii flag)"
    echo "  SNOWFLAKE_USER=your_user              # Service account username"
    echo "  SNOWFLAKE_ROLE=SECURITYADMIN          # Role for applying changes"
    echo "  SNOWFLAKE_PRIVATE_KEY_PATH=~/.ssh/key # Path to private key"
    echo "  SNOWFLAKE_AUTHENTICATOR=SNOWFLAKE_JWT"
    exit 1
fi

# Set account based on --pii flag
if $USE_PII; then
    ACCOUNT_TO_USE="$SNOWFLAKE_ACCOUNT_PII"
    EXCLUDE_RESOURCES=""
    USE_ACCOUNT_USAGE="--use-account-usage"
else
    ACCOUNT_TO_USE="$SNOWFLAKE_ACCOUNT"
    EXCLUDE_RESOURCES="--exclude masking_policy,tag,tag_reference,tag_masking_policy_reference"
    USE_ACCOUNT_USAGE=""
fi

export SNOWFLAKE_ACCOUNT="$ACCOUNT_TO_USE"

# Build uvx command based on branch
if [ -n "$GIT_BRANCH" ]; then
    UVX_CMD="uvx --from snowcap@git+https://github.com/datacoves/snowcap.git@${GIT_BRANCH} --refresh"
else
    UVX_CMD="uvx"
fi

echo "=========="
echo "Using SNOWFLAKE_ACCOUNT=$SNOWFLAKE_ACCOUNT"
if [ -n "$GIT_BRANCH" ]; then
    echo "Using snowcap from branch: $GIT_BRANCH"
fi
if [ -n "$EXCLUDE_RESOURCES" ]; then
    echo "Excluding enterprise-only resources (standard account)"
fi
echo "=========="

# Build config flag (skip if using --plan)
if $USE_PLAN; then
    CONFIG_FLAG=""
    SYNC_FLAG=""
else
    CONFIG_FLAG="--config resources/"
    SYNC_FLAG="--sync_resources role,grant,role_grant,warehouse,database,user,masking_policy,tag,tag_reference,tag_masking_policy_reference"
fi

$UVX_CMD snowcap apply \
    $CONFIG_FLAG \
    $SYNC_FLAG \
    $EXCLUDE_RESOURCES \
    $USE_ACCOUNT_USAGE \
    "$@"

$UVX_CMD snowcap --version
