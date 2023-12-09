# this file uses config info in ~/.snowsql/config to authenticate with snowflake
# see config.sample file for more information

import streamlit as st
# from streamlit_autorefresh import st_autorefresh
import altair as alt


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
        section = "connections"
        auth = dict()

        if 'password' in parser['connections']:
            auth =  {"password": parser.get(section, "password")}
        elif 'private_key_path' in parser['connections']:
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

def get_loan_data():
    conn = getSession()

    data = conn.sql("select * from balboa.l3_loan_analytics.loans_by_state order by NUMBER_OF_LOANS desc") \
        .toPandas()

    return data

def update_viz(data):
    chart = alt.Chart(data).mark_bar().encode(
            alt.X('STATE', sort=alt.EncodingSortField(field="NUMBER_OF_LOANS", op="sum", order='descending')),
            alt.Y('NUMBER_OF_LOANS'),
            color = "STATE"
        )
    st.altair_chart(chart, use_container_width=True)

    st.subheader('Raw data')
    st.dataframe(data)

st.title('Loans by state')

data = get_loan_data()

update_viz(data)

# if count >= 9:
#     st.write("Done Refreshing!!")
# else:
#     st.write(f"Refresh Count: {count}")
#     st.cache_data.clear()
