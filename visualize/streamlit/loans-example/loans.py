# this file uses config info in ~/.snowsql/config to authenticate with snowflake
# see config.sample file for more information

import streamlit as st
import altair as alt

import time
from schedule import every, repeat, run_pending

from database_connection import getSession

def get_loan_data(table_type):
    session = getSession()

    sql= f"select * from balboa_dev.gomezn.loans_by_state__{table_type} order by NUMBER_OF_LOANS desc"

    data = session.sql(sql).toPandas()

    return data

def get_errors():
    session = getSession()

    sql= "select test_name, count_failed from BALBOA_DEV.GOMEZN.STG_TEST_FAILURES"

    data = session.sql(sql)

    return data

st.set_page_config(
    page_title="Standard vs Dynamic Tables",
    page_icon="🏂",
    layout="wide")

alt.themes.enable("dark")

st.title("Loans by state :dollar:")

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

            st.table(get_errors())

    while True:
      run_pending()
      time.sleep(1)
