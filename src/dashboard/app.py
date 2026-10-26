"""
The ERA5 weather dashboard: the explorer of the weather of any place, and an agent that answers questions
about it in plain language.

    uv run streamlit run src/dashboard/app.py
"""

import streamlit as st

# On phones, narrower than 640 px. The map takes a finger dragging on it, not the page: shorter, it leaves room
# to scroll. Two tiles a row, with smaller numbers, their labels wrapped rather than cut short. The hexagon sizes
# and the example questions fit the width. The line on where the search is shows on phones only, where the
# sidebar starts closed.
PHONE_CSS = """
[data-testid="stElementContainer"]:has(.phone-only) {display: none}
@media (max-width: 640px) {
  [data-testid="stElementContainer"]:has(.phone-only) {display: block}
  [data-testid="stMainBlockContainer"] {padding-top: 4rem}
  [data-testid="stMainBlockContainer"] h1 {font-size: 2rem}
  .st-key-map [data-testid="stDeckGlJsonChart"] {height: 380px !important}
  [data-testid="stElementContainer"]:has(> [data-testid="stMetric"]) {
    flex: 0 0 calc(50% - 0.5rem) !important; width: calc(50% - 0.5rem) !important; max-width: none !important;
  }
  [data-testid="stMetricValue"] {font-size: 1.25rem !important}
  [data-testid="stMetricLabel"] *, [data-testid="stMetricDelta"] * {
    white-space: normal !important; overflow: visible !important; text-overflow: clip !important;
  }
  .st-key-hexagons button {padding-left: 0.6rem !important; padding-right: 0.6rem !important; min-width: 0 !important}
  .st-key-examples button {height: auto !important; text-align: left}
  .st-key-examples button * {
    white-space: normal !important; overflow: visible !important; text-overflow: clip !important;
  }
}
"""

st.set_page_config(page_title="Weather anywhere", page_icon="🌍", layout="wide")
# The values of the metrics in the code font of the theme, with digits of one width, as on an instrument panel
st.html(
    "<style>[data-testid='stMetricValue'] {font-family: 'JetBrains Mono', monospace; letter-spacing: -0.03em;"
    f" font-variant-numeric: tabular-nums}} {PHONE_CSS}</style>"
)
page = st.navigation(
    [
        st.Page("explorer.py", title="Weather", icon=":material/map:", default=True),
        st.Page("ask.py", title="Ask the weather", icon=":material/forum:"),
    ],
    position="top",
)
page.run()
