
import configparser, os #re, json
from snowflake.snowpark import Session
from snowflake.snowpark.context import get_active_session

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric import dsa
from cryptography.hazmat.primitives import serialization

def get_rsa_key(key_path):
    with open(key_path, "rb") as key:
        p_key= serialization.load_pem_private_key(
            key.read(),
            password=None,
            backend=default_backend()
        )

    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption())

    return pkb

def getSession():
    try:
        return get_active_session()
    except:
        parser = configparser.ConfigParser()
        filename = os.path.join(os.path.expanduser('~'), ".streamlit/config")
        parser.read(filename)
        section = "connections.dev"
        auth = dict()

        if 'password' in parser[section]:
            auth =  {"password": parser.get(section, "password")}
        elif 'private_key_path' in parser[section]:
            key_path = parser.get(section, "private_key_path")
            key = get_rsa_key(key_path)
            auth =  {"private_key": key}
        else:
            st.write('Did not find password or key')

        pars = {
            "account": parser.get(section, "accountname"),
            "user": parser.get(section, "username"),
            **auth
        }

        return Session.builder.configs(pars).create()
