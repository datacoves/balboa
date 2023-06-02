import yaml

from pathlib import Path
home = str(Path.home())
DBT_PROFILES_YML = Path(home) / ".dbt" / "profiles.yml"

# NOTE: pyarrow needs to be this version for snowpark
# pyarrow==8.0.0
# pip install "snowflake-connector-python[pandas]"

# Needed to create the snowflake session using key based auth
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric import dsa
from cryptography.hazmat.primitives import serialization

def get_connection_properties(profile = 'default', target='dev'):
    with open(DBT_PROFILES_YML, "r") as stream:
        try:
            profiles = yaml.safe_load(stream)
            snowflake_conn_properties = profiles[profile]['outputs'][target]
            snowflake_conn_properties['application'] = "Datacoves_SaaS"

            if "private_key_path" in snowflake_conn_properties:
                # print('Using Key Based Authentication')
                # Read the private key defined in profiles.yml and transform it to the format
                # Snowflake expects
                private_key_path = snowflake_conn_properties['private_key_path']
                with open(private_key_path, "rb") as key:
                    p_key = serialization.load_pem_private_key(
                        key.read(),
                        password=None
                    )

                    pkb = p_key.private_bytes(
                        encoding=serialization.Encoding.DER,
                        format=serialization.PrivateFormat.PKCS8,
                        encryption_algorithm=serialization.NoEncryption())

                snowflake_conn_properties['private_key'] = pkb
            # else:
            #     print('Using Username / Password based Authentication')

            return snowflake_conn_properties

        except yaml.YAMLError as exc:
            print(exc)
