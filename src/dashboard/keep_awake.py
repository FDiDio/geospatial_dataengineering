"""
A visit to the dashboard, as a scheduled workflow makes it (.github/workflows/keep-awake.yml): Community Cloud puts an
app to sleep after 12 hours without traffic, until someone clicks the button of its sleeping page.

A plain request gets a 200 and wakes nothing: an app starts with the JavaScript of its page and its WebSocket. So a real
browser opens the dashboard, clicks the button if it sleeps and stays until the heading of the app shows. The run fails
if it never does, so that an app that crashed shows in the Actions tab too.

    uv run --no-project --with playwright python src/dashboard/keep_awake.py https://<app>.streamlit.app
"""

import sys
import time
from typing import Any

HEADING = "Weather anywhere"
WAKE_BUTTON = "Yes, get this app back up!"
# A sleeping app takes minutes to start
START_S = 300
CHECK_MS = 2_000
# How long a visit stays once the app shows, connected as a visitor is
STAY_MS = 30_000


def find(page: Any, role: str, name: str) -> Any | None:
    """An element of the page or its frames, or None: Community Cloud serves the app in a frame, at /~/+/."""
    for frame in page.frames:
        try:
            element = frame.get_by_role(role, name=name)
            if element.count():
                return element.first
        except Exception:  # a frame gone while the page reloads: the next check looks again
            continue
    return None


def visit(page: Any, url: str, start_s: float = START_S) -> str:
    """
    Open the app, wake it if it sleeps and stay a while: "awake", or "woken up" if it slept. A TimeoutError if the
    app does not show in start_s seconds.
    """
    page.goto(url, wait_until="domcontentloaded", timeout=120_000)
    woken = False
    deadline = time.monotonic() + start_s
    while time.monotonic() < deadline:
        if find(page, "heading", HEADING) is not None:
            page.wait_for_timeout(STAY_MS)
            return "woken up" if woken else "awake"
        button = find(page, "button", WAKE_BUTTON)
        if button is not None:
            button.click()
            woken = True
        page.wait_for_timeout(CHECK_MS)
    raise TimeoutError(f"{HEADING!r} did not show at {url} in {start_s:g} s")


def main() -> None:
    # Here, as the tests and the dashboard do without Playwright
    from playwright.sync_api import sync_playwright

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        print(f"{sys.argv[1]}: {visit(browser.new_page(), sys.argv[1])}")
        browser.close()


if __name__ == "__main__":
    main()
