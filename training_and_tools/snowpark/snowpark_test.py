from snowflake.snowpark import Session
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

def generate_connection_properties(profile = 'default', target='dev'):
    with open(DBT_PROFILES_YML, "r") as stream:
        try:
            profiles = yaml.safe_load(stream)
            snowflake_conn_properties = profiles[profile]['outputs'][target]

            if "private_key_path" in snowflake_conn_properties:
                print('Using Key Based Authentication')
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
            else:
                print('Using Username / Password based Authentication')

            return snowflake_conn_properties

        except yaml.YAMLError as exc:
            print(exc)

connection_properties = generate_connection_properties()

# # build connection to Snowflake
session = Session.builder.configs(connection_properties).create()

# find local pydicom location with : pip show pydicom
# session.add_import("/usr/local/lib/python3.8/site-packages")
# session.add_import("pydicom")

# You can run SQL directly like:
session.sql("select current_warehouse() wh, current_database() db, current_schema() schema, current_version() ver").show()

df = session.table("BALBOA.L1_COUNTRY_DATA._AIRBYTE_RAW_COUNTRY_POPULATIONS").select(["country_code","country_name"]).distinct()
df.describe().show()


rows = df.limit(5)
rows.show()

print('test done')
