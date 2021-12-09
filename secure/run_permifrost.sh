#! /bin/dash

#NOTE: Install the svc_permifrost_key.p8 key to ~/.ssh

export PERMISSION_BOT_USER=SVC_PERMIFROST
export PERMISSION_BOT_ACCOUNT=PX18100.EU-WEST-1
export PERMISSION_BOT_WAREHOUSE=WH_TRANSFORMATION
export PERMISSION_BOT_DATABASE=DEV_RAW
export PERMISSION_BOT_ROLE=SECURITYADMIN
export PERMISSION_BOT_KEY_PATH="/config/.ssh/svc_permifrost_key.p8"
export PERMISSION_BOT_KEY_PASSPHRASE=permifrost
export PATH=$PATH:/config/.local/bin

# This requires installation of our Permifrost fork: python -m pip install git+https://gitlab.com/datacoves/permifrost.git
# python -m pip install git+https://gitlab.com/datacoves/permifrost.git

BRANCH="$(git rev-parse --abbrev-ref HEAD)"
echo "Current Branch is:" $BRANCH

# Only apply role changes from main branch
if [ $BRANCH != 'main' ]
then
    echo 'This script can only be run from the main branch';
    exit 1;
else
    # This allows for running permifrost from any dir and returns to that dir at end of executiuon
    cd /config/workspace/secure/ && permifrost run permifrost.yml ; cd -
fi