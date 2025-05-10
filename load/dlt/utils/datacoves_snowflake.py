import os
import dlt

def set_config_value(key, config_key, env_var_prefix = 'DATACOVES__MAIN_LOAD__'):

    env_var = env_var_prefix + key.upper()

    value = os.getenv(env_var, dlt.config[config_key])

    if key != 'password':
        print(key + ": " +value)
    return value

config_keys = ["host", "database", "warehouse", "role", "username", "private_key"]

db_config = {}
for key in config_keys:
    config_key = "destination.datacoves_snowflake.credentials." + key

    try:
        dlt.config[config_key]
    except dlt.common.configuration.exceptions.ConfigFieldMissingException:
        dlt.config[config_key] = ''

    db_config[key] = set_config_value(key, config_key)
