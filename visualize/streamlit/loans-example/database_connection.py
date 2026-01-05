
import configparser
import os
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

        # Debug: Check if file exists and can be read
        if not os.path.exists(filename):
            print(f"Config file not found at: {filename}")
            raise FileNotFoundError(f"Config file not found at: {filename}")

        parser.read(filename)
        section = "connections"

        # Debug: Check if section exists
        if section not in parser:
            print(f"Available sections: {parser.sections()}")
            raise KeyError(f"Section '{section}' not found in config file")

        auth = dict()

        if 'password' in parser[section]:
            auth =  {"password": parser.get(section, "password")}
        elif 'private_key_file' in parser[section]:
            key_path = parser.get(section, "private_key_file")
            key = get_rsa_key(key_path)
            auth =  {"private_key": key}
        else:
            print('Did not find password or private_key_file')

        pars = {
            "account": parser.get(section, "account"),
            "user": parser.get(section, "user"),
            **auth
        }

        return Session.builder.configs(pars).create()
