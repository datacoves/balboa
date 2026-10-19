#! /bin/dash

export PERMISSION_BOT_ACCOUNT=datacoves-main
export PERMISSION_BOT_WAREHOUSE=WH_TRANSFORMING
export PERMISSION_BOT_DATABASE=BALBOA_DEV
export PERMISSION_BOT_ROLE=SECURITYADMIN
export PERMISSION_BOT_USER="${DATACOVES__USER_EMAIL%@*}"
export PERMISSION_BOT_KEY_PATH="/config/.ssl/prd-private.pem"
export PATH=$PATH:/config/.local/bin

# Runs our Permifrost fork via uvx, no install needed
# Pins: the fork needs SQLAlchemy 1.x (passes raw SQL strings to execute())
PERMIFROST="uvx --from git+https://gitlab.com/datacoves/permifrost.git --with sqlalchemy<2 --with snowflake-connector-python<5 permifrost"

BRANCH="$(git rev-parse --abbrev-ref HEAD)"
echo "Current Branch is:" $BRANCH

cd /config/workspace/secure/permifrost && $PERMIFROST run --parallel 8 permifrost.yml
cd -
