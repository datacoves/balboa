# this file uses config info in ~/.snowsql/config to authenticate with snowflake
# see config.sample file for more information

import streamlit as st
import altair as alt

import time
from schedule import every, repeat, run_pending


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
        filename = os.path.join(os.path.expanduser('~'), ".snowsql/config")
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

def get_loan_data(table_type):
    session = getSession()

    sql= f"select * from balboa_dev.gomezn.loans_by_state__{table_type} order by NUMBER_OF_LOANS desc"

    data = session.sql(sql).toPandas()

    return data


st.set_page_config(layout="wide")
st.title("Standard vs Dynamic Tables :tada:")


with st.empty():
    @repeat(every(5).seconds)
    def strike_details():
        col1, col2 = st.columns(2, gap="large")

        with col1:
            st.subheader('Standard Table')
            data = get_loan_data("standard")

            chart = alt.Chart(data).mark_bar().encode(
                        alt.X('STATE_NAME', sort=alt.EncodingSortField(field="NUMBER_OF_LOANS", op="sum", order='descending')),
                        alt.Y('NUMBER_OF_LOANS'),
                        color = "STATE"
                    )

            st.altair_chart(chart, use_container_width=True)

        with col2:
            st.subheader('Dynamic Table')
            data = get_loan_data("dynamic")
            chart = alt.Chart(data).mark_bar().encode(
                        alt.X('STATE_NAME', sort=alt.EncodingSortField(field="NUMBER_OF_LOANS", op="sum", order='descending')),
                        alt.Y('NUMBER_OF_LOANS'),
                        color = "STATE"
                    )
            st.altair_chart(chart, use_container_width=True)

    while True:
      run_pending()
      time.sleep(1)
