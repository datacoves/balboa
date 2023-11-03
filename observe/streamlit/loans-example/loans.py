# this file uses config info in ~/.snowsql/config to authenticate with snowflake
# see config.sample file for more information

import streamlit as st
# from streamlit_autorefresh import st_autorefresh
import altair as alt


import configparser, os #re, json
from snowflake.snowpark import Session
from snowflake.snowpark.context import get_active_session


def getSession():
    try:
        return get_active_session()
    except:
        parser = configparser.ConfigParser()
        filename = os.path.join(os.path.expanduser('~'), ".snowsql/config")
        parser.read(filename)
        section = "connections"
        pars = {
            "account": parser.get(section, "accountname"),
            "user": parser.get(section, "username"),
            "password": parser.get(section, "password")
        }

        return Session.builder.configs(pars).create()

def get_loan_data():
    conn = getSession()

    data = conn.sql("select * from balboa.l3_loan_analytics.loans_by_state order by NUMBER_OF_LOANS desc") \
        .toPandas()
    st.write('got data')

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
