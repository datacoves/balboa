#! /bin/dash

#NOTE: Install the svc_permifrost_key.p8 key to ~/.ssh

export DATACOVES_USER=gomezn
export PERMISSION_BOT_ACCOUNT=toa80779
export PERMISSION_BOT_WAREHOUSE=WH_TRANSFORMING
export PERMISSION_BOT_DATABASE=RAW
export PERMISSION_BOT_ROLE=SECURITYADMIN
export PERMISSION_BOT_USER="$DATACOVES_USER"
export PERMISSION_BOT_KEY_PATH="/config/.ssh/user_permifrost.p8"
export PERMISSION_BOT_KEY_PASSPHRASE=permifrost
export PATH=$PATH:/config/.local/bin

# This requires installation of our Permifrost fork:
# python -m pip install git+https://gitlab.com/datacoves-oss/permifrost

# GENERATE A KEY AND ADD TO SNWOFLAKE
# Run this in the terminal
# 
# openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out /config/.ssh/user_permifrost.p8
# Use passphrase = permifrost
# openssl rsa -in /config/.ssh/user_permifrost.p8 -pubout -out /config/.ssh/user_permifrost.pub
# cat ~/.ssh/user_permifrost.pub
#
# Run this in Snowflake
# use role securityadmin;
# alter user <Snowflake Username all caps> set rsa_public_key='<Key from above>';


BRANCH="$(git rev-parse --abbrev-ref HEAD)"
echo "Current Branch is:" $BRANCH

# Only apply role changes from main branch
if [ $BRANCH == 'main' ]
then
    echo 'This script can only be run from the main branch';
    exit 1;
else
    # This allows for running permifrost from any dir and returns to that dir at end of executiuon
    cd /config/workspace/secure/ && permifrost run --diff permifrost.yml ; cd -
    #run permifrost.yml ; cd -
fi