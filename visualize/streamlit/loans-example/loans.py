import streamlit as st
import altair as alt

from database_connection import getSession

st.set_page_config(
    page_title="Standard vs Dynamic Tables",
    page_icon="🏂",
    layout="wide",
)

alt.themes.enable("dark")


@st.cache_resource
def get_session():
    return getSession()


def get_loan_data(table_type):
    session = get_session()
    sql = f"select * from balboa.l3_loan_analytics.loans_by_state__{table_type} order by NUMBER_OF_LOANS desc"
    return session.sql(sql).toPandas()


def make_chart(data):
    return alt.Chart(data).mark_bar().encode(
        alt.X("STATE_NAME", sort=alt.EncodingSortField(field="NUMBER_OF_LOANS", op="sum", order="descending")),
        alt.Y("NUMBER_OF_LOANS"),
        color="STATE",
    )


st.title("Loans by state :dollar:")


@st.fragment(run_every="1m")
def loan_charts():
    col1, col2 = st.columns(2, gap="large")

    with col1:
        st.subheader("Standard Table")
        st.altair_chart(make_chart(get_loan_data("standard")), use_container_width=True)

    with col2:
        st.subheader("Dynamic Table")
        st.altair_chart(make_chart(get_loan_data("dynamic")), use_container_width=True)


loan_charts()
