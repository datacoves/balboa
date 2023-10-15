# this file uses config info in ~/.snowsql/config to authenticate with snowflake
# see config.sample file for more information

import streamlit as st
from streamlit_autorefresh import st_autorefresh
import altair as alt

# @st.cache_data(ttl=3)
def get_loan_data():
    conn = st.experimental_connection("snowpark")
    data = conn.query("select * from balboa_dev.gomezn.loans_by_state") \
        .sort_values(by='NUMBER_OF_LOANS', ascending=True) \
        .reset_index(drop=True)
    return data

def update_viz(data):
    # this didnt allow sort, so used altair instead
    # st.bar_chart(
    #     data,
    #     y = "NUMBER_OF_LOANS",
    #     x = "STATE",
    #     color = "STATE"
    #     )

    chart = alt.Chart(data).mark_bar().encode(
            alt.X('STATE', sort=alt.EncodingSortField(field="NUMBER_OF_LOANS", op="sum", order='descending')),
            alt.Y('NUMBER_OF_LOANS'),
            color = "STATE"
        )
    st.altair_chart(chart, use_container_width=True)

    st.subheader('Raw data')
    st.dataframe(data)

st.title('Loans by state')

# Run the autorefresh about every 2000 milliseconds (2 seconds) and stop
# after it's been refreshed 100 times.
count = st_autorefresh(interval=30000, limit=10, key="fizzbuzzcounter")

data = get_loan_data()
update_viz(data)

if count >= 9:
    st.write("Done Refreshing!!")
else:
    st.write(f"Refresh Count: {count}")
    st.cache_data.clear()
