import streamlit as st

st.set_page_config(
    page_title="Snowflake Usage app - About", page_icon="📊", layout="centered"
)

from utils import gui

gui.icon("📊")
st.title("About this app")

st.write(
    """
### How does this app work?

This app is broken down into 3 sections:
- **Compute Insights** has information about your account's credit usage displayed by warehouse, user, etc.
- **Storage Insights** has information about your account's storage usage
- **Data Transfer Insights** has information about data transfer costs

The sidebar also display a date range selector within each page.

### Questions? Comments?

Please ask contact us at [support@datacoves.com](mailto:support@datacoves.com)
"""
)
