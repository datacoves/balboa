#! /bin/dash

export PERMISSION_BOT_ACCOUNT=toa80779
export PERMISSION_BOT_WAREHOUSE=WH_TRANSFORMING
export PERMISSION_BOT_DATABASE=BALBOA_DEV
export PERMISSION_BOT_ROLE=SECURITYADMIN
export PERMISSION_BOT_USER="${DATACOVES__USER_EMAIL%@*}"
export PERMISSION_BOT_KEY_PATH="/config/.ssl/prd-private.pem"
export PATH=$PATH:/config/.local/bin

# This requires installation of our Permifrost fork: python -m pip install git+https://gitlab.com/datacoves/permifrost.git
# python -m pip install git+https://gitlab.com/datacoves/permifrost.git

BRANCH="$(git rev-parse --abbrev-ref HEAD)"
echo "Current Branch is:" $BRANCH

cd /config/workspace/secure/ && permifrost run --parallel 8 permifrost.yml
cd -
