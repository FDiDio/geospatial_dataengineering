"""
The ERA5 weather dashboard: the explorer of the weather of any place, and an agent that answers questions
about it in plain language.

    uv run streamlit run src/dashboard/app.py
"""

import streamlit as st

st.set_page_config(page_title="ERA5 weather", page_icon="🌍", layout="wide")
page = st.navigation(
    [
        st.Page("explorer.py", title="Explorer", icon=":material/map:", default=True),
        st.Page("ask.py", title="Ask the weather", icon=":material/forum:"),
    ],
    position="top",
)
page.run()
