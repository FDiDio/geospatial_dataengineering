"""The dashboard on phones, narrower than 640 px, where Streamlit shows the sidebar over the page."""

import streamlit as st


def is_phone() -> bool:
    """Whether the browser of the session is a phone's, by its user agent: the server does not see the screen."""
    return "Mobi" in (st.context.headers.get("User-Agent") or "")


def close_sidebar(run: int = 0) -> str:
    """
    A script closing the sidebar on a phone, for st.html(..., unsafe_allow_javascript=True): open for the search,
    it would cover the weather it found. Only an open sidebar, on the screen: its button would open a closed one.

    Streamlit runs a script again when its HTML changes, after removing the HTML comments: the number of the run,
    in the script, runs it again. A function keeps its names out of the page, where a second run would declare
    them again.
    """
    return f"""<script>
(() => {{  // run {run}
  const sidebar = document.querySelector('[data-testid="stSidebar"]');
  if (window.innerWidth < 640 && sidebar && sidebar.getBoundingClientRect().right > 0) {{
    document.querySelector('[data-testid="stSidebarCollapseButton"] button')?.click();
  }}
}})();
</script>"""
