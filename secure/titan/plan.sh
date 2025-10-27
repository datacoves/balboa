#!/bin/bash
secure_path=/config/workspace/secure/titan
cd $secure_path

if [ -f .env ]; then
    echo "File .env found."
else
    echo "File .env does not exist. Please create a .env file with the following variables:"
    echo ""
    echo "SNOWFLAKE_ACCOUNT="
    echo "SNOWFLAKE_USER="
    echo "SNOWFLAKE_ROLE="
    echo "SNOWFLAKE_PRIVATE_KEY_PATH="
    echo "SNOWFLAKE_AUTHENTICATOR=SNOWFLAKE_JWT"
    echo ""
    exit 1
fi

export $(cat .env | xargs)

uvx --from titan-core@git+https://github.com/datacoves/titan.git@rc \
    --refresh \
    titan plan \
    --config resources/ \
    --sync_resources role,grant,role_grant
