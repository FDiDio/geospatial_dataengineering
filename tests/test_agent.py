"""The weather agent, its evaluation and its page, without a model: a fake Ollama client scripts its turns."""

import json
import os
import re
from datetime import date

import duckdb
import ollama
import pandas as pd
import pytest
import streamlit as st
from conftest import fake_archive_fetch
from streamlit.testing.v1 import AppTest

from dashboard import agent, agent_eval, archive, places, terrain

APP = os.path.join(os.path.dirname(__file__), "..", "src", "dashboard", "app.py")
ASK = os.path.join(os.path.dirname(__file__), "..", "src", "dashboard", "ask.py")
EXAMPLE = "Quanti giorni sopra 30 °C ha avuto Milano nel luglio 2022?"
HOT_DAYS = "SELECT count(*) AS hot FROM days WHERE temperature_max_c > 50"


def call(tool, **arguments):
    return ollama.Message.ToolCall(function=ollama.Message.ToolCall.Function(name=tool, arguments=arguments))


def reply(content="", *calls):
    message = ollama.Message(role="assistant", content=content, tool_calls=list(calls) or None)
    return ollama.ChatResponse(message=message, prompt_eval_count=100, eval_count=10)


class FakeClient:
    """An Ollama client answering with scripted responses, keeping the messages it was sent at each turn."""

    def __init__(self, *responses):
        self.responses = list(responses)
        self.turns = []

    def chat(self, **kwargs):
        self.turns.append({**kwargs, "messages": list(kwargs["messages"])})
        return self.responses.pop(0)


@pytest.fixture(autouse=True)
def nothing_cached_yet():
    """The agent caches the places it finds and the weather it loads: each test starts without them."""
    for cache in (agent.matches, agent.cached_weather):
        cache.cache_clear()
    yield
    for cache in (agent.matches, agent.cached_weather):
        cache.cache_clear()


@pytest.fixture
def zurich(monkeypatch):
    """The geocoder and Open-Meteo replaced by fakes: the weather of conftest, at Zürich."""
    monkeypatch.setattr(places, "search", lambda name, limit=3: [places.Place("Zürich, Schweiz", 47.37, 8.54, "city")])
    monkeypatch.setattr(archive, "fetch", fake_archive_fetch)


ZURICH = {"name": "Zürich, Schweiz", "latitude": 47.37, "longitude": 8.54}


def test_run_sql_answers_a_select():
    table = pd.DataFrame({"x": [1.0, 2.0, 3.0]})

    result = json.loads(agent.run_sql("SELECT count(*) AS n, max(x) AS top FROM days WHERE x > 1;", "days", table))

    assert result == {"rows": [{"n": 2, "top": 3.0}], "note": "1 row"}


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT month, ANY_VALUE(x) FROM days GROUP BY month",
        "SELECT month, arbitrary(x) FROM days GROUP BY month",
        "SELECT month, FIRST (x) FROM days GROUP BY month",
    ],
)
def test_run_sql_refuses_the_values_of_an_arbitrary_row(sql):
    # One day of each month is not the month: the mean of each month is AVG
    with pytest.raises(ValueError, match="arbitrary row: aggregate the rows with AVG"):
        agent.run_sql(sql, "days", pd.DataFrame({"month": [1, 1], "x": [2.0, 4.0]}))


def test_run_sql_advises_to_aggregate_a_column_left_out_of_group_by():
    with pytest.raises(ValueError) as error:
        agent.run_sql("SELECT month, x FROM days GROUP BY month", "days", pd.DataFrame({"month": [1], "x": [2.0]}))

    assert 'column "x" must appear in the GROUP BY clause' in str(error.value)
    assert "aggregate it with AVG, SUM, MIN, MAX or COUNT" in str(error.value)
    assert "ANY_VALUE" not in str(error.value)


def test_run_sql_refuses_a_result_too_large_to_read():
    # A model shown some of the rows computes from them: it has to aggregate in SQL instead, not cut with LIMIT
    with pytest.raises(ValueError, match="100 rows, more than 40: aggregate them in SQL") as refused:
        agent.run_sql("SELECT * FROM days", "days", pd.DataFrame({"x": range(100)}))
    assert "GROUP BY month(date)" in str(refused.value) and "A LIMIT would keep some of them only" in str(refused.value)
    wide = pd.DataFrame({f"column_{i}": [123.456] * 30 for i in range(12)})
    with pytest.raises(ValueError, match="too long"):
        agent.run_sql("SELECT * FROM days", "days", wide)

    assert len(json.loads(agent.run_sql("SELECT * FROM days LIMIT 40", "days", wide.head(1)))["rows"]) == 1


def test_run_sql_says_when_no_row_matches():
    result = json.loads(agent.run_sql("SELECT x FROM days WHERE x > 5", "days", pd.DataFrame({"x": [1.0]})))

    assert result == {"rows": [], "note": "0 rows: no row matches the conditions of the query, check them"}


def test_the_recipes_of_the_prompt_run_on_the_daily_weather(zurich):
    # Each SQL recipe the model is shown runs, and gives few rows
    recipes = re.findall(
        r"(SELECT .*?FROM (?:days|months)(?: GROUP BY 1(?:\s+ORDER BY 1)?)?)", agent.system_prompt(), re.DOTALL
    )

    for sql in recipes:
        result = json.loads(agent.query_days("Zurich", "2015-01-01", "2025-12-31", " ".join(sql.split())))
        assert 1 <= len(result["rows"]) <= 12

    assert len(recipes) == 4


@pytest.mark.parametrize(
    "sql",
    [
        "DROP TABLE days",
        "INSERT INTO days VALUES (4)",
        "SELECT 1; SELECT 2",
        "SELECT * FROM read_csv('pyproject.toml')",
        "SELECT * FROM 'pyproject.toml'",
        "WITH t AS (SELECT * FROM read_text('README.md')) SELECT * FROM t",
    ],
)
def test_run_sql_reads_the_table_only(sql):
    with pytest.raises((ValueError, duckdb.Error)):
        agent.run_sql(sql, "days", pd.DataFrame({"x": [1]}))


@pytest.mark.parametrize(
    ("start", "end", "error"),
    [
        ("1939-12-31", "1940-01-31", "within"),
        ("2022-07-31", "2022-07-01", "within"),
        ("1950-01-01", "1990-01-01", "too long"),
    ],
)
def test_query_days_refuses_a_period_out_of_the_data(zurich, start, end, error):
    with pytest.raises(ValueError, match=error):
        agent.query_days("Zurich", start, end, HOT_DAYS)


def test_query_days_runs_sql_on_the_daily_weather_of_a_place_by_name(zurich):
    result = json.loads(agent.query_days("Zurich", "2022-07-01", "2022-07-31", HOT_DAYS + " AND source = 'ERA5'"))

    # High of 25 °C + the day of the month: above 50 °C from the 26th to the 31st
    assert result == {"place": ZURICH, "rows": [{"hot": 6}], "note": "1 row"}


def test_query_days_takes_three_decades_in_a_query(zurich):
    sql = "SELECT count(*) AS days, count(DISTINCT year(date)) AS years FROM days"

    result = json.loads(agent.query_days("Zurich", "1991-01-01", "2020-12-31", sql))

    assert result["rows"] == [{"days": 10958, "years": 30}]


def test_query_days_gives_the_totals_of_each_month(zurich):
    # 2 mm on the even days: 15 wet days and 30 mm in each January, 14 and 28 mm in each February, leap or not
    sql = (
        "SELECT month, AVG(precipitation_mm) AS rain, AVG(wet_days) AS wet, AVG(temperature_max_c) AS high, "
        "SUM(day_count) AS days FROM months WHERE month <= 2 GROUP BY 1 ORDER BY 1"
    )

    result = json.loads(agent.query_days("Zurich", "2015-01-01", "2025-12-31", sql))

    assert result["rows"] == [
        {"month": 1, "rain": 30.0, "wet": 15.0, "high": 41.0, "days": 11 * 31},
        {"month": 2, "rain": 28.0, "wet": 14.0, "high": 39.6, "days": 11 * 28 + 3},
    ]


def test_query_days_takes_coordinates_without_the_geocoder(monkeypatch):
    monkeypatch.setattr(places, "search", lambda name, limit=3: pytest.fail("the coordinates were geocoded"))
    monkeypatch.setattr(archive, "fetch", fake_archive_fetch)

    result = json.loads(agent.query_days("47.37, 8.54", "2022-07-01", "2022-07-31", HOT_DAYS))

    assert result["place"] == {"name": "47.3700, 8.5400", "latitude": 47.37, "longitude": 8.54}


def test_query_hours_reads_the_local_clock_times(zurich):
    sql = "SELECT hour(time) AS hour, temperature_c FROM hours WHERE time::DATE = '2022-07-01' ORDER BY 2 DESC LIMIT 1"

    result = agent.query_hours("Zurich", "2022-07-01", "2022-07-02", sql)

    # 15 °C + half a degree an hour of the local day, the warmest at 23:00 in Zürich, 21:00 UTC
    assert json.loads(result)["rows"] == [{"hour": 23, "temperature_c": 26.5}]
    with pytest.raises(ValueError, match="too long"):
        agent.query_hours("Zurich", "2022-07-01", "2023-08-15", sql)


def test_find_place_gives_the_matches(zurich, monkeypatch):
    assert json.loads(agent.find_place("Zurich")) == [{**ZURICH, "kind": "city"}]
    monkeypatch.setattr(places, "search", lambda name, limit=3: [])
    assert agent.find_place("Atlantis") == "No place found for 'Atlantis'."
    with pytest.raises(ValueError, match="no place found for 'Atlantis'"):
        agent.query_days("Atlantis", "2022-07-01", "2022-07-31", HOT_DAYS)


def test_a_follow_up_question_loads_the_weather_once(zurich, monkeypatch):
    loaded = []
    monkeypatch.setattr(archive, "fetch", lambda *args, **kwargs: loaded.append(args) or fake_archive_fetch(*args))

    agent.query_days("Zurich", "2022-07-01", "2022-07-31", HOT_DAYS)
    agent.query_days("Zurich", "2022-07-01", "2022-07-31", "SELECT max(temperature_max_c) FROM days")

    assert len(loaded) == 1


def test_places_are_geocoded_once(monkeypatch):
    searched = []
    monkeypatch.setattr(places, "search", lambda name, limit=3: searched.append(name) or [places.Place("Zürich", 1, 2)])

    agent.find_place("Zurich")
    agent.locate("Zurich")

    assert searched == ["Zurich"]


def test_cell_altitude_gives_the_altitude_of_the_hexagon(zurich, monkeypatch):
    monkeypatch.setattr(terrain, "cell_altitude", lambda cell: terrain.Altitude(412.04, 390.0, 870.0, 49))

    assert json.loads(agent.cell_altitude("Zurich")) == {
        "place": ZURICH,
        "mean_m": 412.0,
        "min_m": 390.0,
        "max_m": 870.0,
    }


def test_tool_descriptions_list_the_columns_of_the_tables():
    assert "temperature_max_c" in agent.query_days.__doc__ and "{columns}" not in agent.query_days.__doc__
    assert "sunshine_s" in agent.query_hours.__doc__ and "{columns}" not in agent.query_hours.__doc__


def test_run_tool_gives_errors_back_to_the_model():
    unknown = agent.run_tool("delete_everything", {})
    wrong = agent.run_tool("query_days", {"place": "Zurich"})

    assert unknown.error and unknown.result.startswith("Error: no tool 'delete_everything'")
    assert wrong.error and "missing" in wrong.result


def test_ask_runs_the_tools_the_model_calls(zurich):
    client = FakeClient(
        reply("", call("find_place", name="Zurich")),
        reply("", call("query_days", place="Zurich", start="2022-07-01", end="2022-07-31", sql=HOT_DAYS)),
        reply("Zurigo ha avuto 6 giorni sopra 50 °C nel luglio 2022."),
    )
    steps = []

    answer = agent.ask("Giorni sopra 50 °C a Zurigo, luglio 2022?", client=client, on_step=steps.append,
                       today=date(2026, 10, 24))  # fmt: skip

    assert answer.text == "Zurigo ha avuto 6 giorni sopra 50 °C nel luglio 2022."
    assert [step.tool for step in answer.steps] == ["find_place", "query_days"] and steps == answer.steps
    assert json.loads(answer.steps[1].result)["rows"] == [{"hot": 6}]
    assert (answer.prompt_tokens, answer.output_tokens) == (300, 30)
    assert answer.unverified == []  # 6 from the data, 50 and 2022 from the question
    # Each turn sends the conversation so far: the system prompt, the question, each call and its result
    first, last = client.turns[0], client.turns[-1]
    assert "Saturday 24 October 2026" in first["messages"][0]["content"]
    assert first["messages"][1] == {"role": "user", "content": "Giorni sopra 50 °C a Zurigo, luglio 2022?"}
    assert last["messages"][-1] == {"role": "tool", "content": answer.steps[1].result, "tool_name": "query_days"}
    assert first["tools"] == list(agent.TOOLS.values()) and first["options"]["temperature"] == 0


def test_ask_gives_a_failed_query_back_to_the_model(zurich):
    period = {"place": "Zurich", "start": "2022-07-01", "end": "2022-07-31"}
    client = FakeClient(
        reply("", call("query_days", **period, sql="SELECT max(temperature) FROM days")),
        reply("", call("query_days", **period, sql="SELECT max(temperature_max_c) AS top FROM days")),
        reply("56,0 °C"),
    )

    answer = agent.ask("Massima di luglio 2022 a Zurigo?", client=client)

    assert [step.error for step in answer.steps] == [True, False]
    assert "temperature" in client.turns[1]["messages"][-1]["content"]  # the error of DuckDB, for the model to fix
    assert json.loads(answer.steps[1].result)["rows"] == [{"top": 56.0}]


def test_ask_answers_after_the_earlier_turns():
    history = [{"role": "user", "content": "Milano, luglio 2022?"}, {"role": "assistant", "content": "30 giorni."}]
    client = FakeClient(reply("E 25 nel 2023."))

    agent.ask("E nel 2023?", history=history, client=client)

    assert client.turns[0]["messages"][1:] == [*history, {"role": "user", "content": "E nel 2023?"}]


def test_ask_gives_up_after_the_last_step(zurich):
    client = FakeClient(*[reply("", call("find_place", name="Zurich"))] * agent.MAX_STEPS)

    answer = agent.ask("?", client=client)

    assert answer.text.startswith(f"No answer within {agent.MAX_STEPS} steps")
    assert len(answer.steps) == agent.MAX_STEPS


def test_ask_flags_the_numbers_the_data_did_not_give(zurich):
    period = {"place": "Zurich", "start": "2016-01-01", "end": "2025-12-31"}
    client = FakeClient(
        reply("", call("query_days", **period, sql="SELECT avg(temperature_mean_c) AS mean FROM days")),
        reply("Media annua 2016-2025: 35,7 °C, circa 5,2 °C più di prima."),
    )

    answer = agent.ask("Temperatura media degli ultimi 10 anni a Zurigo?", client=client)

    assert json.loads(answer.steps[0].result)["rows"] == [{"mean": 35.7}]  # to one decimal, to copy as it is
    assert answer.unverified == ["5,2"]


def test_ask_stops_before_the_conversation_overflows_the_context(zurich, monkeypatch):
    question = "Giorni sopra 50 °C a Zurigo?"
    start = agent.conversation_tokens([{"content": agent.system_prompt()}, {"content": question}])
    # Room for the question, not for a tool result more
    monkeypatch.setattr(agent, "CONTEXT", start + agent.CONTEXT_RESERVE + 5)
    client = FakeClient(reply("", call("find_place", name="Zurich")), reply("Never asked"))

    answer = agent.ask(question, client=client)

    assert answer.text == agent.NOT_IN_CONTEXT and len(client.turns) == 1


@pytest.mark.parametrize(
    ("text", "number"),
    [("21,4 °C", 21.4), ("−21.4 °C", -21.4), ("1.234,5 mm", 1234.5), ("1,234.5 mm", 1234.5), ("alle 17:00", 17.0)],
)
def test_numbers_reads_italian_and_english_numbers(text, number):
    assert number in agent.numbers(text)


def test_numbers_reads_the_parts_of_a_date():
    assert agent.numbers("2024-10-29 e 2016-2025") == {2024.0, 10.0, 29.0, 2016.0, 2025.0}


@pytest.mark.parametrize(
    ("text", "flagged"),
    [
        ("14,8 °C nel 2016", []),  # rounded to one decimal
        ("circa 15 °C", []),  # rounded to units
        ("alle 17:00", []),
        ("14,9 °C", ["14,9"]),
        ("in media 5,2 °C", ["5,2"]),
    ],
)
def test_unverified_flags_the_numbers_the_data_did_not_give(text, flagged):
    step = agent.Step("query_days", {"start": "2016-01-01"}, '{"rows": [{"mean": 14.826, "hour": 17}]}', 0.1)

    assert agent.unverified(text, "Media di Milano?", [step]) == flagged


@pytest.mark.parametrize(
    ("answer", "expected", "kind", "passed"),
    [
        ("30 giorni", [30], "count", True),
        ("30 giorni", [31], "count", False),
        ("-21,7 °C", [-21.7], "decimal", True),
        ("21,7 °C", [-21.7], "decimal", False),
        ("-21,4 °C", [-21.7], "decimal", False),
        ("1567 mm", [1567.3], "total", True),
        ("1500 mm", [1567.3], "total", False),
        ("21,5 °C nel 2003 e 21,2 °C nel 2022", [21.5, 21.2], "decimal", True),
        ("21,5 °C nel 2003", [21.5, 21.2], "decimal", False),
    ],
)
def test_grade_wants_every_expected_number(answer, expected, kind, passed):
    assert agent_eval.grade(answer, expected, kind) is passed


def test_judge_checks_the_place_and_the_value():
    truths = {}
    sums = []
    case = agent_eval.Case("sum", "?", "Zurich", lambda lat, lon: sums.append(1) or [round(lat + lon, 2)])
    matches = [places.Place("Zürich", 47.37, 8.54)]

    def answer(text, *steps):
        """An answer with the data tools it called, each at a point or failed."""
        return agent.Answer(text, "model", [
            agent.Step(tool, {"place": "Zurich"}, "Error: no" if error else json.dumps({"place": point}), 0.1, error)
            for tool, point, error in steps
        ])  # fmt: skip

    at = {"latitude": 47.38, "longitude": 8.54}  # another match of the name, near enough
    assert agent_eval.judge(case, answer("55.92"), matches, truths) == ("no data", [])
    assert agent_eval.judge(case, answer("55.92", ("query_days", at, True)), matches, truths) == ("no data", [])
    down = answer("Riprova più tardi.", ("query_days", at, True))
    down.steps[0].result = "Error: <urlopen error [Errno 11001] getaddrinfo failed>"
    assert agent_eval.judge(case, down, matches, truths) == ("error", [])  # the network, not the model
    far = {"latitude": 46.2, "longitude": 6.15}
    assert agent_eval.judge(case, answer("52.35", ("query_days", far, False)), matches, truths) == ("wrong place", [])
    assert agent_eval.judge(case, answer("55.92", ("query_days", at, False)), matches, truths) == (None, [55.92])
    assert agent_eval.judge(case, answer("55.5", ("query_days", at, False)), matches, truths) == (
        "wrong value",
        [55.92],
    )
    assert len(sums) == 1  # the expected numbers of a point are computed once


def test_evaluate_asks_again_after_an_error_records_it_and_goes_on(monkeypatch, tmp_path):
    monkeypatch.setattr(places, "search", lambda name, limit=3: [places.Place("Zürich", 47.37, 8.54)])
    monkeypatch.setattr(agent_eval, "RETRY_AFTER_S", 0)
    asked = []

    def ask(question, model, think):
        asked.append(question)
        if question == "down" or asked == ["blip"]:
            raise ConnectionError("Failed to connect to Ollama")
        result = json.dumps({"place": {"latitude": 47.37, "longitude": 8.54}})
        return agent.Answer("55.91", model, [agent.Step("query_days", {"place": "Zurich"}, result, 0.1)])

    monkeypatch.setattr(agent, "ask", ask)
    cases = [
        agent_eval.Case(question, question, "Zurich", lambda lat, lon: [lat + lon]) for question in ("blip", "down")
    ]
    record = str(tmp_path / "eval.jsonl")

    results = agent_eval.evaluate(cases, ["qwen3:8b"], False, record=record)

    # Asked twice each: the first passes the second time, the second fails twice
    assert asked == ["blip", "blip", "down", "down"]
    assert [(result["case"], result["failure"]) for result in results] == [("blip", None), ("down", "error")]
    assert results[1]["answer"] == "Error: Failed to connect to Ollama"
    assert len(agent_eval.latest_results(record)) == 2


def test_the_evaluation_keeps_the_final_era5_days_on_disk(monkeypatch, tmp_path):
    monkeypatch.setattr(agent_eval, "CACHE", str(tmp_path))
    loaded = []

    def fetch(latitude, longitude, start, end, source=archive.ERA5):
        loaded.append((start, end, source.name))
        return fake_archive_fetch(latitude, longitude, start, end, source)

    cached = agent_eval.kept_on_disk(fetch)
    recent = archive.last_day() - date.resolution * 10

    first = cached(47.37, 8.54, date(2022, 7, 1), date(2022, 7, 31))
    again = cached(47.37, 8.54, date(2022, 7, 1), date(2022, 7, 31))
    cached(47.37, 8.54, recent, recent)
    cached(47.37, 8.54, recent, recent)
    cached(47.37, 8.54, date(2022, 7, 1), date(2022, 7, 31), archive.IFS)

    # The past loaded once and read from disk after; the preliminary days and the other sources each time
    assert loaded == [(date(2022, 7, 1), date(2022, 7, 31), "ERA5"), (recent, recent, "ERA5"),
                      (recent, recent, "ERA5"), (date(2022, 7, 1), date(2022, 7, 31), "ECMWF IFS")]  # fmt: skip
    assert again.daily.equals(first.daily) and len(list(tmp_path.iterdir())) == 1


def test_latest_results_keeps_the_last_run_of_each_case(tmp_path):
    record = tmp_path / "eval.jsonl"
    rows = [
        {"model": "qwen3:8b", "think": False, "case": "a", "passed": False, "failure": "wrong value", "seconds": 7.0},
        {"model": "qwen3:8b", "think": False, "case": "b", "passed": True, "failure": None, "seconds": 5.0},
        {"model": "qwen3:8b", "think": False, "case": "a", "passed": True, "failure": None, "seconds": 6.0},
        {"model": "qwen3:8b", "think": True, "case": "a", "passed": False, "failure": "no data", "seconds": 20.0},
    ]
    record.write_text("".join(json.dumps(row) + "\n" for row in rows))

    latest = agent_eval.latest_results(str(record))

    assert len(latest) == 3 and latest[~latest["think"].astype(bool)]["passed"].all()
    assert agent_eval.latest_results(str(tmp_path / "missing.jsonl")).empty


def test_summary_scores_each_model():
    run = {"model": "m", "think": False}
    results = [
        {**run, "passed": True, "failure": None, "seconds": 4.0, "tool_calls": 2, "tool_errors": 0},
        {**run, "passed": False, "failure": "wrong place", "seconds": 6.0, "tool_calls": 3, "tool_errors": 1},
    ]

    row = agent_eval.summary(results).loc[("m", False)]

    assert (row["passed"], row["cases"], row["accuracy"], row["mean_seconds"]) == (1, 2, 0.5, 5.0)
    assert row["wrong place"] == 1


def test_available_models_offers_those_of_agent_models(monkeypatch):
    class Server:
        def list(self):
            names = ("kimi-k3", "gpt-oss:20b", "gpt-oss:120b")
            return ollama.ListResponse(models=[ollama.ListResponse.Model(model=name) for name in names])

    assert agent.available_models(Server()) == ["gpt-oss:120b", "gpt-oss:20b", "kimi-k3"]

    monkeypatch.setattr(agent, "MODELS", ["gpt-oss:20b", "qwen3:8b"])

    assert agent.available_models(Server()) == ["gpt-oss:20b"]


def fake_ollama(monkeypatch, models=("qwen3:4b", "qwen3:8b"), ask=None):
    """Ollama replaced by fakes for the page of the agent, and an evaluation of Qwen3 8B."""

    def available_models():
        if models is None:
            raise ConnectionError("Failed to connect to Ollama")
        return list(models)

    monkeypatch.setattr(agent, "available_models", available_models)
    monkeypatch.setattr(agent, "ask", ask or (lambda *args, **kwargs: None))
    monkeypatch.setattr(agent_eval, "latest_results", lambda: pd.DataFrame(
        {"model": ["qwen3:8b"] * 2, "think": [False] * 2, "passed": [True, False], "seconds": [6.0, 8.0]}))  # fmt: skip


def ask_page(monkeypatch, **fakes):
    fake_ollama(monkeypatch, **fakes)
    app = AppTest.from_file(ASK)
    app.run(timeout=60)
    return app


def test_dashboard_has_a_page_to_ask_the_weather(monkeypatch, tmp_path):
    monkeypatch.setenv("DUCKDB_PATH", str(tmp_path / "missing.duckdb"))
    fake_ollama(monkeypatch, models=None)
    app = AppTest.from_file(APP)
    app.run(timeout=60)

    assert app.title[0].value == "ERA5 weather explorer"

    app.switch_page("ask.py").run(timeout=60)

    assert not app.exception
    assert app.title[0].value == "Ask the weather"


def test_ask_page_says_how_to_start_ollama(monkeypatch):
    app = ask_page(monkeypatch, models=None)

    assert not app.exception
    assert "ollama pull qwen3:8b" in app.info[0].value and not app.chat_input


def test_ask_page_answers_with_the_steps(monkeypatch):
    asked = []

    def ask(question, history, model, think, on_step):
        asked.append((question, history, model, think))
        step = agent.Step("query_days", {"latitude": 45.46, "longitude": 9.19, "sql": "SELECT 30 AS hot"},
                          '{"rows": [{"hot": 30}], "note": "1 row"}', 1.2)  # fmt: skip
        on_step(step)
        return agent.Answer(f"Risposta {len(asked)}: 30 giorni.", model, [step], seconds=6.4)

    app = ask_page(monkeypatch, ask=ask)

    assert not app.exception
    assert app.sidebar.selectbox[0].value == "qwen3:8b"
    assert app.sidebar.caption[0].value == "Evaluation: 1 of 2 questions right, 7.0 s each"
    assert len(app.main.button) == 4 and app.main.button[0].label == EXAMPLE

    app.chat_input[0].set_value("Quanti giorni sopra 30 °C a Milano nel luglio 2022?").run(timeout=60)

    assert not app.exception
    assert [message.name for message in app.chat_message] == ["user", "assistant"]
    assert app.status[0].label == "How I got it: 1 tool call, 6.4 s, qwen3:8b"
    assert "SELECT 30 AS hot" in [code.value for code in app.code]
    assert app.markdown[-1].value == "Risposta 1: 30 giorni."
    assert not app.main.button  # the examples are gone

    app.chat_input[0].set_value("E nel 2023?").run(timeout=60)

    # The earlier turns go to the agent as their text
    history = [{"role": "user", "content": asked[0][0]}, {"role": "assistant", "content": "Risposta 1: 30 giorni."}]
    assert asked[1] == ("E nel 2023?", history, "qwen3:8b", False)
    assert len(app.chat_message) == 4

    app.sidebar.button[0].click().run(timeout=60)

    assert not app.chat_message and len(app.main.button) == 4  # a new conversation, with the examples


def test_ask_page_asks_an_example_and_flags_the_numbers_not_in_the_data(monkeypatch):
    asked = []

    def ask(question, *args, **kwargs):
        asked.append(question)
        return agent.Answer("Circa 5,2 °C.", "qwen3:8b", unverified=["5,2"])

    app = ask_page(monkeypatch, ask=ask)
    assert not app.warning

    app.main.button[0].click().run(timeout=60)

    assert asked == [EXAMPLE]
    assert app.status[0].label == "Answered without querying the data"
    assert app.warning[0].value.startswith("Not in the data the agent read: 5,2.")


def test_ask_page_shows_when_the_model_fails(monkeypatch):
    def ask(*args, **kwargs):
        raise ollama.ResponseError("model 'qwen3:8b' not found")

    app = ask_page(monkeypatch, ask=ask)
    app.chat_input[0].set_value("Che tempo fa?").run(timeout=60)

    assert not app.exception
    assert app.status[0].label == "The model did not answer" and "not found" in app.error[0].value
    assert app.chat_input  # the page stays, to ask again


def test_ask_page_says_when_ollama_cloud_does_not_answer(monkeypatch):
    monkeypatch.setattr(agent, "CLOUD", True)
    app = ask_page(monkeypatch, models=None)

    assert not app.exception
    assert app.caption[0].value.startswith("An open model served by Ollama Cloud")
    assert app.error[0].value == "Ollama Cloud does not answer: Failed to connect to Ollama" and not app.chat_input


def test_ask_page_answers_a_number_of_questions_an_hour(monkeypatch):
    monkeypatch.setenv("AGENT_QUESTIONS_PER_HOUR", "2")
    st.cache_resource.clear()  # the questions asked by other tests
    app = ask_page(monkeypatch, ask=lambda question, *args, **kwargs: agent.Answer("Sì.", "qwen3:8b"))
    for question in ["Piove a Roma?", "E a Milano?"]:
        app.chat_input[0].set_value(question).run(timeout=60)

    assert len(app.chat_message) == 4 and not app.warning

    app.chat_input[0].set_value("E a Napoli?").run(timeout=60)

    assert not app.exception
    assert len(app.chat_message) == 4  # not answered
    assert app.warning[0].value == (
        "This demo answers 2 questions an hour, to all its visitors together: ask again in 60 minutes."
    )
