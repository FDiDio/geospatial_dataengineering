import dataclasses
import io
import json
import math
import os
import urllib.error
import urllib.parse
from datetime import date, timedelta

import h3
import pandas as pd
import pytest
import streamlit as st
from conftest import fake_archive_fetch
from streamlit.testing.v1 import AppTest

from dashboard import archive, places, terrain
from dashboard.queries import (
    BLUES,
    COLD_ARM,
    LAYERS,
    NEUTRAL,
    WARM_ARM,
    colorize,
    hex_to_rgb,
    hexagon_size,
    pick_cells,
    pick_window,
    place_window,
    scale_colors,
)

APP = os.path.join(os.path.dirname(__file__), "..", "src", "dashboard", "app.py")


def rgb(color):
    return list(hex_to_rgb(color))


def test_diverging_scale_is_neutral_at_its_center_and_clipped_at_its_bounds():
    layer = LAYERS["temperature_anomaly_c"]  # -10 .. 0 .. 10
    colors = colorize(pd.Series([-50.0, -10.0, 0.0, 10.0, 50.0]), layer)

    assert [c[:3] for c in colors] == [rgb(COLD_ARM[0]), rgb(COLD_ARM[0]), rgb(NEUTRAL["light"]), rgb(WARM_ARM[-1]),
                                       rgb(WARM_ARM[-1])]  # fmt: skip
    assert all(c[3] == 220 for c in colors)


def test_asymmetric_diverging_range_keeps_the_midpoint_on_the_center():
    layer = LAYERS["temperature_max_c"]  # -40 .. 0 .. 45
    assert colorize(pd.Series([0.0]), layer)[0][:3] == rgb(NEUTRAL["light"])


def test_dark_theme_uses_the_dark_midpoint():
    assert NEUTRAL["dark"] in scale_colors(LAYERS["temperature_mean_c"], "dark")


def test_sequential_scale_goes_from_light_to_dark():
    layer = LAYERS["precipitation_mm"]
    low, high = colorize(pd.Series([0.0, 500.0]), layer)
    assert low[:3] == rgb(BLUES[0]) and high[:3] == rgb(BLUES[-1])


@pytest.mark.parametrize("key", list(LAYERS))
def test_every_layer_has_a_valid_scale(key):
    layer = LAYERS[key]
    assert layer.low < layer.high
    if layer.scale == "diverging":
        assert layer.low < layer.center < layer.high


def test_place_search_parses_the_nominatim_answer(monkeypatch):
    sent = {}

    def urlopen(request, timeout):
        sent["url"], sent["headers"] = request.full_url, dict(request.header_items())
        answer = [
            {
                "display_name": "Milano, Lombardia, Italia",
                "lat": "45.4641943",
                "lon": "9.1896346",
                "addresstype": "city",
            }
        ]
        return io.BytesIO(json.dumps(answer).encode())

    monkeypatch.setattr(places.urllib.request, "urlopen", urlopen)
    [milano] = places.search("Milano", language="it")

    assert milano == places.Place("Milano, Lombardia, Italia", 45.4641943, 9.1896346, "city")
    assert milano.short_name == "Milano"
    assert milano.label == "Milano, Lombardia, Italia (city, 45.46° 9.19°)"
    assert "q=Milano" in sent["url"] and "format=jsonv2" in sent["url"]
    assert sent["headers"]["Accept-language"] == "it" and "era5" in sent["headers"]["User-agent"]


def archive_answer(first_hour, offset_hours, temperatures, timezone):
    """
    An answer of the archive: hourly values from first_hour, all shifted by the same UTC offset. The wind
    blows at 10 km/h from 10° on the even hours and from 350° on the odd ones, with gusts of 20 to 22 km/h.
    The humidity is 50 % on the even hours and 60 % on the odd ones, under 80 % of clouds, with 10 minutes
    of sun an hour. The hours 31 to 42 are the daylight ones: the sky is overcast, but for a thunderstorm in
    the night, at the hour 28, and some light rain by day, at the hour 36.
    """
    times = pd.date_range(first_hour, periods=len(temperatures), freq="h")

    def where_published(values):
        return [None if t is None else value for t, value in zip(temperatures, values, strict=True)]

    return {
        "latitude": 40.75,
        "longitude": -74.0,
        "elevation": 10.0,
        "timezone": timezone,
        "utc_offset_seconds": offset_hours * 3600,
        "hourly": {
            "time": [time.strftime("%Y-%m-%dT%H:%M") for time in times],
            "temperature_2m": temperatures,
            "precipitation": where_published([0.1] * len(times)),
            "wind_speed_10m": where_published([10.0] * len(times)),
            "wind_direction_10m": where_published([10.0 if n % 2 == 0 else 350.0 for n in range(len(times))]),
            "wind_gusts_10m": where_published([20.0 + n % 3 for n in range(len(times))]),
            "relative_humidity_2m": where_published([50.0 + n % 2 * 10 for n in range(len(times))]),
            "cloud_cover": where_published([80.0] * len(times)),
            "sunshine_duration": where_published([600.0] * len(times)),
            "weather_code": where_published([95 if n == 28 else 61 if n == 36 else 3 for n in range(len(times))]),
            # Astronomical: also for the hours not published yet
            "is_day": [1 if 31 <= n <= 42 else 0 for n in range(len(times))],
        },
    }


@pytest.fixture
def archive_server(monkeypatch):
    """Serve a canned archive answer, recording the URL of the request."""
    served = {}

    def urlopen(request, timeout):
        served["url"] = request.full_url
        return io.BytesIO(json.dumps(served["answer"]).encode())

    monkeypatch.setattr(archive.urllib.request, "urlopen", urlopen)
    return served


def test_local_days_follow_daylight_saving_time(archive_server):
    # New York, 6 Nov 2022: clocks go back from 02:00 EDT to 01:00 EST. The archive gives the hours of
    # 5 to 7 Nov at today's offset, -4 h; the temperature is the number of hours since 5 Nov 04:00 UTC.
    archive_server["answer"] = archive_answer("2022-11-05", -4, [float(n) for n in range(72)], "America/New_York")
    weather = archive.fetch(40.71, -74.01, date(2022, 11, 6), date(2022, 11, 6))

    for param in ("models=era5", "timezone=auto", "start_date=2022-11-05", "end_date=2022-11-07"):
        assert param in archive_server["url"]
    hours = weather.hourly
    # 00:00 EDT (04:00 UTC) to 23:00 EST (04:00 UTC the next day): 25 hours, 01:00 twice
    assert len(hours) == 25 and hours["temperature_c"].tolist() == [float(n) for n in range(24, 49)]
    assert hours["time"].dt.strftime("%H:%M %Z").tolist()[1:3] == ["01:00 EDT", "01:00 EST"]
    # Noon EST is 17:00 UTC, which the archive calls 13:00
    assert hours.loc[hours["time"].dt.hour == 12, "temperature_c"].tolist() == [37.0]
    assert weather.daily.to_dict("records") == [
        {
            "date": pd.Timestamp("2022-11-06"),
            "temperature_max_c": 48.0,
            "temperature_mean_c": 36.0,
            "temperature_min_c": 24.0,
            "precipitation_mm": pytest.approx(2.5),
            "wind_speed_mean_kmh": 10.0,
            "wind_speed_max_kmh": 10.0,
            "wind_gusts_max_kmh": 22.0,
            # 13 hours from 10° and 12 from 350°: their vectors add up to the north, a little east of it,
            # where the mean of the angles would be the south
            "wind_direction_deg": pytest.approx(0.404, abs=1e-3),
            # 13 hours at 50 % and 12 at 60 %
            "humidity_mean_pct": pytest.approx(54.8),
            "humidity_min_pct": 50.0,
            "humidity_max_pct": 60.0,
            "cloud_cover_mean_pct": 80.0,
            "sunshine_h": pytest.approx(25 * 10 / 60),
            # The light rain by day, not the thunderstorm of the night
            "weather_code": 61,
            "source": "ERA5",
        }
    ]


@pytest.mark.parametrize(
    ("code", "by_day", "icon_and_label"),
    [
        (0, True, ("☀️", "Clear")),
        (0, False, ("🌙", "Clear")),
        (2, True, ("⛅", "Partly cloudy")),
        (63, True, ("🌧️", "Rain")),
        (95, False, ("⛈️", "Thunderstorm")),
        (4, True, ("", "–")),
        (float("nan"), True, ("", "–")),
    ],
)
def test_the_sky_of_a_weather_code(code, by_day, icon_and_label):
    assert archive.sky(code, by_day) == icon_and_label


def test_the_weather_of_a_day_sums_its_hours_up():
    nan = float("nan")
    days = pd.DataFrame(
        [
            # daylight clouds, all clouds, daylight storm, precipitation, all codes ≥ 51, daylight ones, fog hours
            (10, 10, nan, 0.0, nan, nan, 0),  # sunny
            (32, 40, nan, 0.0, nan, nan, 0),  # a few clouds: an overcast hour does not make it overcast
            (56, 56, nan, 0.0, nan, nan, 0),
            (85, 85, nan, 0.0, nan, nan, 0),
            (90, 90, nan, 6.0, 63, 53, 0),  # a wet day: its daylight precipitation
            (40, 60, nan, 12.0, 95, nan, 0),  # a wet day with a thunderstorm at night only
            (60, 60, 96, 0.5, 96, 96, 0),  # a thunderstorm by day, wet or not
            (27, 30, nan, 0.0, nan, nan, 4),  # a foggy morning
            (70, 70, nan, 2.0, nan, nan, 0),  # a wet day with no precipitation code: light rain
            (nan, 90, nan, 0.0, nan, nan, 0),  # a polar night: the clouds of all the hours
        ],
        columns=["daylight_clouds", "cloud_cover_mean_pct", "daylight_storm_code", "precipitation_mm",
                 "precipitation_code", "daylight_precipitation_code", "fog_hours"],
    )  # fmt: skip

    assert archive.weather_of_days(days).tolist() == [0, 1, 2, 3, 53, 95, 96, 45, 61, 3]


@pytest.mark.parametrize(
    ("degrees", "name"), [(0.0, "N"), (11.0, "N"), (12.0, "NNE"), (90.0, "E"), (247.5, "WSW"), (359.0, "N")]
)
def test_compass_names(degrees, name):
    assert archive.compass(degrees) == name


def test_compass_of_no_direction():
    assert archive.compass(float("nan")) == "–"


def test_days_not_published_yet_are_left_out(archive_server):
    # Milano in summer, where today's offset is right: 1 Jul is complete, 2 Jul has only 5 hours
    temperatures = [20.0] * 22 + [float(n) for n in range(24)] + [21.0] * 5 + [None] * 21
    archive_server["answer"] = archive_answer("2022-06-30T02:00", 2, temperatures, "Europe/Rome")
    weather = archive.fetch(45.46, 9.19, date(2022, 7, 1), date(2022, 7, 2))

    assert weather.daily["date"].tolist() == [pd.Timestamp("2022-07-01")]
    assert (weather.daily.loc[0, "temperature_min_c"], weather.daily.loc[0, "temperature_max_c"]) == (0.0, 23.0)
    assert len(weather.hourly) == 24 + 5


def test_archive_ends_six_days_ago():
    assert archive.last_day(date(2026, 10, 23)) == date(2026, 10, 17)
    assert archive.last_complete_day(date(2026, 10, 23)) == date(2026, 10, 16)
    assert archive.last_forecast_day(date(2026, 10, 24)) == date(2026, 11, 7)


@pytest.fixture
def recorded_fetch(monkeypatch):
    """archive.fetch replaced by the fake archive, recording the source and the days of each request."""
    calls = []

    def fetch(latitude, longitude, start, end, source=archive.ERA5):
        calls.append((source.name, start, end))
        return fake_archive_fetch(latitude, longitude, start, end, source)

    monkeypatch.setattr(archive, "fetch", fetch)
    return calls


NOW = pd.Timestamp("2026-10-24 10:00", tz="UTC")  # 12:00 in Zurich, ERA5 up to 18 Oct


def test_era5_then_the_recent_runs_of_ecmwf_ifs_then_its_forecast(recorded_fetch):
    weather = archive.fetch_with_forecast(47.37, 8.54, date(2026, 10, 1), date(2026, 11, 7), now=NOW)

    # ERA5 up to its last day, IFS from the day before it: the last local day of ERA5 may be incomplete
    assert recorded_fetch == [
        ("ERA5", date(2026, 10, 1), date(2026, 10, 18)),
        ("ECMWF IFS", date(2026, 10, 17), date(2026, 11, 7)),
    ]
    days = weather.daily.set_index(weather.daily["date"].dt.date)["source"]
    assert days.index.is_unique and list(days.index) == [date(2026, 10, 1) + timedelta(n) for n in range(38)]
    assert set(days[: date(2026, 10, 18)]) == {"ERA5"}
    assert set(days[date(2026, 10, 19) : date(2026, 10, 23)]) == {"ECMWF IFS"}
    assert set(days[date(2026, 10, 24) :]) == {"Forecast"}  # today included
    hours = weather.hourly
    assert hours["time"].is_monotonic_increasing and hours["time"].is_unique
    assert set(hours.loc[hours["time"] < NOW, "source"]) == {"ERA5", "ECMWF IFS"}
    assert set(hours.loc[hours["time"] >= NOW, "source"]) == {"Forecast"}
    assert set(weather.points) == {"ERA5", "ECMWF IFS"}


def test_a_past_year_is_all_era5_and_the_coming_days_all_forecast(recorded_fetch):
    past = archive.fetch_with_forecast(47.37, 8.54, date(2022, 1, 1), date(2022, 12, 31), now=NOW)
    assert [call[0] for call in recorded_fetch] == ["ERA5"] and set(past.daily["source"]) == {"ERA5"}

    recorded_fetch.clear()
    coming = archive.fetch_with_forecast(47.37, 8.54, date(2026, 11, 1), date(2026, 11, 7), now=NOW)
    assert [call[0] for call in recorded_fetch] == ["ECMWF IFS"] and set(coming.daily["source"]) == {"Forecast"}


def test_place_window_stays_within_the_latitudes():
    assert place_window(45.0, 9.0) == (37.0, 53.0, -7.0, 25.0)
    assert place_window(85.0, 0.0)[1] == 90.0
    assert place_window(45.0, 9.0, half_height=4.0) == (41.0, 49.0, 1.0, 17.0)


@pytest.mark.parametrize(
    ("text", "coordinates"),
    [
        ("45.46, 9.19", (45.46, 9.19)),
        ("45.46 9.19", (45.46, 9.19)),
        ("45,46 9,19", (45.46, 9.19)),
        (" -33.87;151.21 ", (-33.87, 151.21)),
        ("40.71, 285.99", (40.71, -74.01)),
        ("45, 9", (45.0, 9.0)),
        ("Milano", None),
        ("45.46", None),
        ("95, 9", None),
        ("Route 66, 9", None),
    ],
)
def test_coordinates_are_parsed(text, coordinates):
    parsed = places.parse_coordinates(text)
    assert parsed == (None if coordinates is None else pytest.approx(coordinates))


@pytest.mark.parametrize(
    ("latitude", "longitude", "dms"),
    [
        (45.4641943, 9.1896346, "45°27′51″N 9°11′23″E"),  # Milano
        (40.7127281, -74.0060152, "40°42′46″N 74°00′22″W"),  # New York
        (-33.8698439, 151.2082848, "33°52′11″S 151°12′30″E"),  # Sydney
        (0.0, -0.0001, "0°00′00″N 0°00′00″W"),
    ],
)
def test_coordinates_in_degrees_minutes_and_seconds(latitude, longitude, dms):
    assert places.format_dms(latitude, longitude) == dms


def test_a_place_at_coordinates_is_named_by_them():
    place = places.at(45.46, 9.19)
    assert (place.name, place.short_name, place.kind) == ("45.4600, 9.1900", "45.4600, 9.1900", "coordinates")


def test_the_clicked_point_is_the_center_of_the_clicked_hexagon():
    cell = "851f99cffffffff"
    state = {"selection": {"indices": {"pick-3": [7]}, "objects": {"pick-3": [{"h3": cell, "value_text": "…"}]}}}

    assert places.clicked_point(state, ["pick-3", "weather-3"]) == pytest.approx(h3.cell_to_latlng(cell))
    # A selection on layers renamed since: a click already taken
    assert places.clicked_point(state, ["pick-4", "weather-4"]) is None
    assert places.clicked_point({"selection": {"indices": {}, "objects": {}}}, ["pick-0"]) is None
    assert places.clicked_point(None, ["pick-0"]) is None


def test_reverse_geocoding_names_the_point_or_nothing_at_sea(monkeypatch):
    answers = [{"display_name": "Milano, Lombardia, Italia"}, {"error": "Unable to geocode"}]
    monkeypatch.setattr(
        places.urllib.request, "urlopen", lambda request, timeout: io.BytesIO(json.dumps(answers.pop(0)).encode())
    )

    assert places.reverse(45.46, 9.19) == "Milano, Lombardia, Italia"
    assert places.reverse(40.0, -40.0) is None


def test_pick_cells_cover_the_world_or_a_window():
    world = pick_cells(None, 2)
    assert len(world) == 5882 and world["h3"].is_unique

    bbox = (44.46, 46.46, 7.19, 11.19)
    window = pick_cells(bbox, 5)
    centers = [h3.cell_to_latlng(cell) for cell in window["h3"]]
    assert len(window) > 100 and all(h3.get_resolution(cell) == 5 for cell in window["h3"])
    assert all(bbox[0] <= lat <= bbox[1] and bbox[2] <= lon <= bbox[3] for lat, lon in centers)
    assert window["value_text"][0].endswith("° · click for its weather")
    assert pick_cells(bbox, 3, "click to go there")["value_text"][0].endswith("° · click to go there")


def test_around_a_place_coarser_hexagons_take_the_map_there(monkeypatch, tmp_path):
    taken = []
    monkeypatch.setattr(places, "clicked_point", lambda state, layers: taken.append(list(layers)))
    app = run_app(monkeypatch, tmp_path)
    app.sidebar.text_input[0].set_value("Zurich").run(timeout=60)

    deck = json.loads(app.get("deck_gl_json_chart")[0].proto.json)
    clickable = [layer for layer in deck["layers"] if layer["id"].endswith("-0") and layer.get("pickable")]
    hexagons = {layer["id"].removesuffix("-0"): layer["data"] for layer in clickable}

    # The world, the hexagons two sizes coarser than the place's around it, and the fine ones, each drawn over
    # the coarser: a click takes the topmost
    assert list(hexagons) == ["world", "near", "pick"]
    resolutions = {name: {h3.get_resolution(row["h3"]) for row in data} for name, data in hexagons.items()}
    assert resolutions == {"world": {2}, "near": {5}, "pick": {7}}
    assert hexagons["near"][0]["value_text"].endswith("click to go there")
    assert hexagons["pick"][0]["value_text"].endswith("click for its weather")
    assert "near-0" in taken[-1]


def test_the_clickable_window_keeps_its_number_of_hexagons_at_every_size():
    assert pick_window(45.0, 9.0, 5) == place_window(45.0, 9.0, 4.0)
    assert pick_window(45.0, 9.0, 7)[1] == pytest.approx(45.0 + 4.0 / 7)
    counts = {resolution: len(pick_cells(pick_window(45.0, 9.0, resolution), resolution)) for resolution in range(3, 8)}
    assert all(3000 < count < 7000 for count in counts.values()), counts


def test_hexagon_sizes_from_corner_to_corner():
    assert [hexagon_size(resolution) for resolution in (3, 5, 7)] == ["138 km", "20 km", "2.8 km"]


def test_monthly_statistics_of_a_daily_history():
    weather = fake_archive_fetch(45.5, 9.25, date(2022, 1, 1), date(2022, 12, 31))
    months = archive.monthly(weather.daily)

    assert len(months) == 12 and months["days"].sum() == 365
    july = months.set_index("month").loc[pd.Timestamp("2022-07-01")]
    # Highs 25 + day of the month, lows 15 + day, means 20 + day, 2 mm on even days
    assert (july["temperature_max_mean_c"], july["temperature_min_mean_c"], july["temperature_mean_c"]) == (
        41.0,
        31.0,
        36.0,
    )
    assert (july["temperature_max_c"], july["temperature_min_c"]) == (56.0, 16.0)
    assert (july["precipitation_mm"], july["wet_days"], july["days"]) == (30.0, 15, 31)
    year = archive.summary(weather.daily)
    assert (year["days"], year["temperature_max_c"], year["temperature_min_c"]) == (365, 56.0, 16.0)


def test_the_altitude_of_a_cell_samples_49_points_of_it(monkeypatch):
    cell = "851f99cffffffff"  # Milano
    sent = {}

    def urlopen(request, timeout):
        query = urllib.parse.parse_qs(urllib.parse.urlparse(request.full_url).query)
        sent["points"] = list(zip(query["latitude"][0].split(","), query["longitude"][0].split(","), strict=True))
        heights = [100.0 + n for n in range(len(sent["points"]))]
        return io.BytesIO(json.dumps({"elevation": heights}).encode())

    monkeypatch.setattr(terrain.urllib.request, "urlopen", urlopen)
    altitude = terrain.cell_altitude(cell)

    assert len(sent["points"]) == 49
    # A few points of the edge fall just outside the cell, the H3 hierarchy being approximate
    cells = [h3.latlng_to_cell(float(lat), float(lon), 5) for lat, lon in sent["points"]]
    assert cells.count(cell) >= 45 and set(cells) <= set(h3.grid_disk(cell, 1))
    assert altitude == terrain.Altitude(mean=124.0, min=100.0, max=148.0, points=49)


def run_app(monkeypatch, tmp_path, fetch=fake_archive_fetch):
    """The dashboard without a warehouse, Nominatim and Open-Meteo replaced by fakes."""
    monkeypatch.setenv("DUCKDB_PATH", str(tmp_path / "missing.duckdb"))
    monkeypatch.setattr(places, "search", lambda query, language: [places.Place("Zürich, Schweiz", 47.37, 8.54)])
    monkeypatch.setattr(places, "reverse", lambda latitude, longitude, language: "Zürich, Schweiz")
    monkeypatch.setattr(archive, "fetch", fetch)
    monkeypatch.setattr(terrain, "cell_altitude", lambda cell: terrain.Altitude(412.0, 390.0, 870.0, 49))
    st.cache_data.clear()
    app = AppTest.from_file(APP)
    app.run(timeout=60)
    return app


def metrics(block):
    return {metric.label: metric.value for metric in block.metric}


def test_dashboard_shows_a_place_without_a_warehouse(monkeypatch, tmp_path):
    app = run_app(monkeypatch, tmp_path)
    assert not app.exception and not app.expander

    app.sidebar.text_input[0].set_value("Zurich")
    app.sidebar.date_input[0].set_value(date(2022, 7, 15)).run(timeout=60)

    assert not app.exception
    assert app.subheader[0].value == "Zürich"
    # The exact coordinates of the city: under the search field, to copy, and in degrees, minutes and seconds
    assert ":material/location_on: 47.370000, 8.540000" in [caption.value for caption in app.sidebar.caption]
    assert app.code[0].value == "47.370000, 8.540000"
    assert any("47°22′12″N 8°32′24″E" in markdown.value for markdown in app.markdown)
    day = metrics(app.main)
    assert (day["High"], day["Low"], day["Precipitation"]) == ("40.0 °C", "30.0 °C", "0.0 mm")
    altitude = next(metric for metric in app.metric if metric.label == "Mean altitude")
    assert (altitude.value, altitude.delta) == ("412 m", "390–870 m")
    # The finest hexagon by default, 2.8 km across
    assert any(":material/hexagon: 5 km² hexagon" in markdown.value for markdown in app.markdown)
    month_tab, year_tab, _ = app.tabs
    assert [tab.label.split(": ", 1)[-1] for tab in app.tabs] == [
        "Month · July 2022",
        "Year · 2022",
        "Day · 15 Jul 2022",
    ]
    month = metrics(month_tab)
    assert (month["Average high"], month["Average low"], month["Average"]) == ("41.0 °C", "31.0 °C", "36.0 °C")
    assert (month["Highest"], month["Lowest"]) == ("56.0 °C", "16.0 °C")
    assert (month["Total precipitation"], month["Wet days"]) == ("30.0 mm", "15 of 31")
    # Winds of 10 to 14 km/h, 12.0 on average, from the west; gusts of 30 km/h + the day of the month
    wind = next(metric for metric in app.metric if metric.label == "Wind")
    assert (wind.value, wind.delta) == ("10.0 km/h", "from W · gusts 45 km/h")
    assert (month["Average wind"], month["Strongest gust"]) == ("12.0 km/h", "61.0 km/h")
    # 15 Jul, an odd day: mainly clear, 6 h of sun under 50 % of clouds, 65 % of humidity
    assert any(markdown.value == "### 🌤️ Mainly clear" for markdown in app.markdown)
    sunshine = next(metric for metric in app.metric if metric.label == "Sunshine")
    humidity = next(metric for metric in app.metric if metric.label == "Humidity")
    assert (sunshine.value, sunshine.delta, humidity.value, humidity.delta) == (
        "6.0 h",
        "clouds 50 %",
        "65.0 %",
        "40–90 %",
    )
    # July: 6 h of sun + the day modulo 3 a day, 60 % of humidity + the day modulo 10
    assert (month["Total sunshine"], month["Average humidity"]) == ("217.0 h", "64.4 %")
    assert metrics(year_tab)["Wet days"] == "179 of 365"  # the even days: 15 a month, 14 in February
    days, months, hours = (frame.value for frame in app.dataframe)
    assert len(days) == 31 and list(days.columns[:4]) == ["Day", "Sky", "High", "Average"]
    assert list(days["Sky"][:2]) == ["🌤️ Mainly clear", "🌦️ Light rain"]
    assert len(months) == 12 and list(months.columns[:4]) == ["Month", "Average high", "Average low", "Average"]
    assert months.set_index("Month").loc["Jul", "Average high"] == 41.0
    assert len(hours) == 24


def test_a_place_typed_or_clicked_as_coordinates(monkeypatch, tmp_path):
    clicks = []
    monkeypatch.setattr(places, "clicked_point", lambda state, layers: clicks.pop() if clicks else None)
    app = run_app(monkeypatch, tmp_path)

    app.sidebar.text_input[0].set_value("47.37, 8.54").run(timeout=60)
    assert not app.exception
    assert app.subheader[0].value == "47.3700, 8.5400"
    assert "Near Zürich, Schweiz" in [caption.value for caption in app.caption]
    assert any("weather model, 47.25° 8.50°" in caption.value for caption in app.caption)

    # A click on the map writes the center of the clicked hexagon in the place field
    clicks.append((45.46, 9.19))
    app.run(timeout=60)
    assert not app.exception
    assert app.sidebar.text_input[0].value == "45.4600, 9.1900"
    assert app.subheader[0].value == "45.4600, 9.1900"


def test_a_failed_server_is_asked_again_and_a_wrong_request_is_not(monkeypatch):
    answers = [urllib.error.HTTPError("url", 502, "Bad Gateway", {}, None), io.BytesIO(b'{"ok": true}')]

    def urlopen(request, timeout):
        answer = answers.pop(0)
        if isinstance(answer, Exception):
            raise answer
        return answer

    monkeypatch.setattr(archive.urllib.request, "urlopen", urlopen)
    monkeypatch.setattr(archive.time, "sleep", lambda seconds: None)
    assert archive.get_json("https://example.org") == {"ok": True}

    answers[:] = [urllib.error.HTTPError("url", 400, "Bad Request", {}, None), io.BytesIO(b"{}")]
    with pytest.raises(urllib.error.HTTPError):
        archive.get_json("https://example.org")


def test_a_request_over_the_limit_says_why_at_once(monkeypatch):
    reason = b'{"error": true, "reason": "Hourly API request limit exceeded. Please try again in the next hour."}'
    answers = [urllib.error.HTTPError("url", 429, "Too Many Requests", {}, io.BytesIO(reason))]

    def urlopen(request, timeout):
        raise answers.pop(0)

    monkeypatch.setattr(archive.urllib.request, "urlopen", urlopen)
    with pytest.raises(archive.RequestLimit, match=r"Hourly API request limit exceeded. Please try again"):
        archive.get_json("https://example.org")
    assert not answers  # asked once


def test_the_past_shows_when_the_forecast_fails(monkeypatch, tmp_path):
    def fetch(latitude, longitude, start, end, source=archive.ERA5):
        if source == archive.IFS:
            raise urllib.error.URLError("forecast server down")
        return fake_archive_fetch(latitude, longitude, start, end, source)

    app = run_app(monkeypatch, tmp_path, fetch)
    app.sidebar.text_input[0].set_value("Zurich")
    app.sidebar.date_input[0].set_value(archive.last_complete_day()).run(timeout=60)

    assert not app.exception
    warning = app.warning[0].value
    assert "Part of the weather is missing" in warning and "recent data and forecast: " in warning
    assert any("Historical data" in markdown.value for markdown in app.markdown)
    assert "High" in metrics(app.main)


def test_today_is_a_forecast_left_out_of_the_statistics(monkeypatch, tmp_path):
    app = run_app(monkeypatch, tmp_path)
    app.sidebar.text_input[0].set_value("Zurich").run(timeout=60)  # on today, the default day

    assert not app.exception
    assert any(":material/online_prediction: Forecast]" in markdown.value for markdown in app.markdown)
    days = app.dataframe[0].value
    assert "Forecast" in set(days["Source"]) and set(days["Source"]) <= {"Historical", "Recent", "Forecast"}
    gone_by = int((days["Source"] != "Forecast").sum())
    if gone_by:
        assert metrics(app.tabs[0])["Wet days"].endswith(f"of {gone_by}")


def test_zoom_buttons_and_hexagon_sizes(monkeypatch, tmp_path):
    app = run_app(monkeypatch, tmp_path)
    app.sidebar.text_input[0].set_value("Zurich").run(timeout=60)
    # Hexagons of 2.8 km by default: the place gets its resolution 7 cell, zoomed in to keep them easy to click
    fine = 7.5 + 2 * math.log2(math.sqrt(7))
    assert app.session_state["zoom"] == pytest.approx(fine)
    assert any(":material/hexagon: 5 km² hexagon" in markdown.value for markdown in app.markdown)

    def press(label):
        next(button for button in app.button if button.label == label).click().run(timeout=60)
        assert not app.exception
        return app.session_state["zoom"]

    assert [press("Zoom in"), press("Zoom in"), press("Zoom out"), press("Recenter")] == pytest.approx(
        [fine + 1, fine + 2, fine + 1, fine]
    )

    # Hexagons of 20 km: the resolution 5 cell, and the map zooms out to keep them as large
    app.button_group[0].set_value(5).run(timeout=60)
    assert not app.exception
    assert any(":material/hexagon: 242 km² hexagon" in markdown.value for markdown in app.markdown)
    assert app.session_state["zoom"] == pytest.approx(7.5)


def test_the_chosen_place_stays_when_the_day_changes(monkeypatch, tmp_path):
    # Nominatim names the city and the state of New York alike
    city = places.Place("New York, United States", 40.71, -74.01, "city")
    state = places.Place("New York, United States", 43.16, -75.84, "state")
    app = run_app(monkeypatch, tmp_path)
    monkeypatch.setattr(places, "search", lambda query, language: [city, state])
    app.sidebar.text_input[0].set_value("New York").run(timeout=60)
    assert app.sidebar.selectbox[0].value == city

    app.sidebar.date_input[0].set_value(date(2022, 7, 15)).run(timeout=60)

    assert not app.exception
    assert app.sidebar.selectbox[0].value == city
    assert any("weather model, 40.75° -74.00°" in caption.value for caption in app.caption)


def test_a_day_published_up_to_the_evening_has_no_daily_values(monkeypatch, tmp_path):
    def fetch_up_to_19(latitude, longitude, start, end, source=None):
        """The archive west of Greenwich: the hours of the last day stop at 19:00, so that day is left out."""
        weather = fake_archive_fetch(latitude, longitude, start, end, source)
        hours = weather.hourly["time"]
        hourly = weather.hourly[(hours.dt.date < end) | (hours.dt.hour <= 19)]
        return dataclasses.replace(weather, hourly=hourly, daily=weather.daily[weather.daily["date"].dt.date < end])

    app = run_app(monkeypatch, tmp_path, fetch_up_to_19)
    app.sidebar.text_input[0].set_value("Zurich")
    app.sidebar.date_input[0].set_value(date(2022, 12, 31)).run(timeout=60)

    assert not app.exception
    notice = app.info[-1].value  # after the one of the missing warehouse
    assert "not complete yet" in notice and "19:00" in notice
    assert "High" not in metrics(app.main)
    days, months, hours = (frame.value for frame in app.dataframe)
    assert (len(days), len(months), len(hours)) == (30, 12, 20)
    assert any("up to 19:00" in caption.value for caption in app.caption)
