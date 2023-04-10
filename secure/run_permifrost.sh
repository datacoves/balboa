#! /bin/dash

export PERMISSION_BOT_ACCOUNT=toa80779
export PERMISSION_BOT_WAREHOUSE=WH_TRANSFORMING
export PERMISSION_BOT_DATABASE=BALBOA_DEV
export PERMISSION_BOT_ROLE=SECURITYADMIN
export PERMISSION_BOT_USER="${DATACOVES_USER_EMAIL%@*}"
export PERMISSION_BOT_KEY_PATH="/config/.ssl/prd-private.pem"
export PATH=$PATH:/config/.local/bin

# This requires installation of our Permifrost fork: python -m pip install git+https://gitlab.com/datacoves/permifrost.git
# python -m pip install git+https://gitlab.com/datacoves/permifrost.git

BRANCH="$(git rev-parse --abbrev-ref HEAD)"
echo "Current Branch is:" $BRANCH

# Only apply role changes from main branch
if [ $BRANCH != 'main' ]; then
    echo 'This script can only be run from the main branch'
    exit 1
else
    # This allows for running permifrost from any dir and returns to that dir at end of executiuon
    cd /config/workspace/secure/ && permifrost run permifrost.yml
    cd -
fi
