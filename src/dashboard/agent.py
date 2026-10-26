"""
An agent that answers questions about the weather of any place in plain language, with an open model served
by Ollama: by default Qwen3 8B, which fits in the 8 GB of a laptop GPU.

The model picks the tools and writes SQL; the tools compute every number from the same sources as the
dashboard (ERA5, then ECMWF IFS and its forecast). The model never does the arithmetic, so the numbers of
its answers are the numbers of the data; the numbers of an answer that the data did not give are flagged.

    uv run python src/dashboard/agent.py "Quanti giorni sopra 30 °C ha avuto Milano nel luglio 2022?"
"""

import functools
import json
import os
import re
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date
from typing import Any

import duckdb
import h3
import ollama
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dashboard import archive, places, queries, terrain

MODEL = os.environ.get("AGENT_MODEL", "qwen3:8b")
HOST = os.environ.get("OLLAMA_HOST", "http://localhost:11434")
# Model turns for a question: an answer takes 2 or 3, a query to fix one more
MAX_STEPS = 8
# Tokens of context: the prompt and the tools take about 1,000, each turn up to about 1,500 more. Qwen3 8B
# with 8,192 still fits in 8 GB of GPU memory.
CONTEXT = 8192
# A conversation stops when it would leave fewer tokens than this for the next turn: else Ollama would drop
# its beginning, the instructions and the question, without a word, and the model answer without them
CONTEXT_RESERVE = 1024
# Upper estimates of the tokens of a conversation: the descriptions of the tools, and the characters of a
# token at worst (JSON numbers make short tokens)
TOOL_TOKENS = 800
CHARS_PER_TOKEN = 2.5
# How long Ollama keeps the model loaded after a question: loading it takes about 12 s
KEEP_ALIVE = "30m"
# The largest query result given to the model. An answer needs a few values; a model shown part of the rows
# computes from the part, so a larger result is refused with the advice to aggregate it in SQL.
MAX_ROWS = 40
MAX_RESULT_CHARS = 4000
# The longest periods of a query: 31 years of days, a climate normal and one more (30 years load in about
# 8 s), a year of hours
MAX_DAYS = 31 * 366
MAX_HOURS_DAYS = 366
NOT_IN_CONTEXT = "The data of this question no longer fits the context of the model: ask about fewer values."

DAY_COLUMNS = """\
date (local day, a DATE), temperature_max_c, temperature_mean_c, temperature_min_c, precipitation_mm (total of
the day: the precipitation of a month or a year is the SUM of its days),
sunshine_h (hours of sun), humidity_mean_pct, humidity_min_pct, humidity_max_pct, cloud_cover_mean_pct,
wind_speed_mean_kmh, wind_speed_max_kmh, wind_gusts_max_kmh, wind_direction_deg (dominant, where it blows
from), weather_code (WMO), source (where the values come from: 'ERA5', 'ECMWF IFS' for the past days ERA5 has
not published yet, 'Forecast' from today on; filter by it only when the question asks)"""
HOUR_COLUMNS = """\
time (local time, a TIMESTAMP: the hours of a day are WHERE time::DATE = 'YYYY-MM-DD', the hour is
hour(time)), temperature_c, precipitation_mm, humidity_pct, cloud_cover_pct, sunshine_s (seconds of sun in
the hour), wind_speed_kmh, wind_direction_deg, wind_gusts_kmh, weather_code (WMO), is_day, source"""

# A number as written, not the hyphen of a date or a range: 2024-10-29 is 2024, 10 and 29
NUMBER = re.compile(r"(?<![\d.,])[-−]?\d[\d.,]*")
# Aggregates of an arbitrary row of a group. DuckDB suggests ANY_VALUE for a column left out of GROUP BY, and a
# model taking it up gets the values of one day of each month instead of their mean.
ARBITRARY_ROW = re.compile(r"\b(any_value|arbitrary|first|last)\s*\(", re.IGNORECASE)
ANY_VALUE_HINT = re.compile(r"Either add it to the GROUP BY list, or use .*? is not important\.", re.DOTALL)


def system_prompt(today: date | None = None) -> str:
    today = today or date.today()
    return f"""\
You answer questions about the weather of any place on Earth, from 1940 to the next two weeks.
Today is {today:%A %d %B %Y}.

Use the tools for every number:
1. query_days runs SQL on the daily weather of a place, query_hours on its hourly weather; cell_altitude
   gives the altitude of its area. Give them the place by its name, as the question writes it, or by its
   coordinates. Use find_place only to choose among places of the same name.
2. Let SQL compute every value of the answer, in one query when it can, as in these recipes:
   - a mean or a total over the period: SELECT AVG(temperature_mean_c), SUM(precipitation_mm) FROM days
   - values by year: SELECT year(date) AS year, AVG(temperature_mean_c) FROM days GROUP BY 1 ORDER BY 1
   - each month of the year over several years, 12 rows: SELECT month(date) AS month, AVG(temperature_max_c)
     AS high, SUM(precipitation_mm) / COUNT(DISTINCT year(date)) AS precipitation_mm FROM days GROUP BY 1
     ORDER BY 1
   - the day of the highest value: SELECT arg_max(date, temperature_max_c) AS day, MAX(temperature_max_c)
     FROM days
   Never compute or estimate a number yourself, nor from some of the rows. LIMIT keeps the top rows of an
   ORDER BY; it never makes a result fit, as the rows it drops are part of the answer.
3. If a query fails, read the error and fix the query. If no query gives the answer, say so.

ERA5 reanalysis covers the days up to {archive.last_day(today):%d %B %Y}; the ECMWF IFS model the days after,
and its forecast from today to {archive.last_forecast_day(today):%d %B %Y}. The tables hold all of these days
together, each with its source: query them by date, by source only when the question asks. Say when values
are a forecast.
"The last N years" are the N full years before {today.year}.

Answer briefly, in the language of the question: the place the tools found, the period, and the values as the
tools give them, rounded to one decimal already, with their units."""


@functools.lru_cache(maxsize=256)
def matches(name: str) -> tuple[places.Place, ...]:
    """The places of a name, cached: the geocoder allows one request a second."""
    return tuple(places.search(name, limit=3))


def locate(place: str) -> places.Place:
    """A place by its coordinates, "45.46, 9.19", or by its name, the first match."""
    coordinates = places.parse_coordinates(place)
    if coordinates is not None:  # to 0.1 m, without the float error of bringing the longitude to -180..180
        return places.at(*(round(degrees, 6) for degrees in coordinates))
    found = matches(place)
    if not found:
        raise ValueError(f"no place found for {place!r}: try another name, or coordinates")
    return found[0]


@functools.lru_cache(maxsize=8)
def cached_weather(latitude: float, longitude: float, first: date, last: date, hour: int) -> archive.Weather:
    return archive.fetch_with_forecast(latitude, longitude, first, last)


def weather(latitude: float, longitude: float, first: date, last: date) -> archive.Weather:
    """
    The weather of a place from first to last, kept for an hour, as the forecast changes: a follow-up
    question loads nothing again, and Open-Meteo counts a request of 30 years as about 780 of its free calls.
    """
    return cached_weather(latitude, longitude, first, last, int(time.time() // 3600))


def described(place: places.Place) -> dict[str, Any]:
    return {"name": place.name, "latitude": place.latitude, "longitude": place.longitude}


def find_place(name: str) -> str:
    """Find the places of a name: up to 3 matches, the most relevant first. The other tools take the name
    themselves, and its first match: use this tool to choose among places of the same name.

    Args:
        name: The place, e.g. "Milano", "New York" or "Cortina d'Ampezzo".
    """
    found = matches(name)
    if not found:
        return f"No place found for {name!r}."
    return json.dumps([{**described(place), "kind": place.kind} for place in found], ensure_ascii=False)


def query_days(place: str, start: str, end: str, sql: str) -> str:
    """Run a DuckDB SELECT on the daily weather of a place, the table `days`, from start to end.
    The table holds that place only, without a place column, and has: {columns}.

    Args:
        place: The place: its name, e.g. "Milano", or its coordinates, e.g. "45.46, 9.19".
        start: The first day, YYYY-MM-DD, from 1940-01-01.
        end: The last day, YYYY-MM-DD, at most 31 years after start.
        sql: A SELECT on the table `days` computing the values of the answer, at most 40 rows.
    """
    where = locate(place)
    first, last = period(start, end, MAX_DAYS)
    days = weather(where.latitude, where.longitude, first, last).daily
    return run_sql(sql, "days", days, where)


def query_hours(place: str, start: str, end: str, sql: str) -> str:
    """Run a DuckDB SELECT on the hourly weather of a place, the table `hours`, from start to end: for the
    questions about the time of day. The table holds that place only, without a place column, and has: {columns}.

    Args:
        place: The place: its name, e.g. "Milano", or its coordinates, e.g. "45.46, 9.19".
        start: The first day, YYYY-MM-DD.
        end: The last day, YYYY-MM-DD, at most a year after start.
        sql: A SELECT on the table `hours` computing the values of the answer, at most 40 rows.
    """
    where = locate(place)
    first, last = period(start, end, MAX_HOURS_DAYS)
    hours = weather(where.latitude, where.longitude, first, last).hourly
    # DuckDB reads the local clock times as they are written
    return run_sql(sql, "hours", hours.assign(time=hours["time"].dt.tz_localize(None)), where)


def cell_altitude(place: str) -> str:
    """The altitude of the area of a place, its H3 hexagon of 2.8 km, about 5 km²: mean, lowest and highest, in m.

    Args:
        place: The place: its name, e.g. "Milano", or its coordinates, e.g. "45.46, 9.19".
    """
    where = locate(place)
    altitude = terrain.cell_altitude(h3.latlng_to_cell(where.latitude, where.longitude, queries.PLACE_RESOLUTION))
    return json.dumps(
        {"place": described(where), "mean_m": round(altitude.mean, 1), "min_m": altitude.min, "max_m": altitude.max},
        ensure_ascii=False,
    )


TOOLS: dict[str, Callable[..., str]] = {tool.__name__: tool for tool in (find_place, query_days, query_hours,
                                                                          cell_altitude)}  # fmt: skip
# The columns of the tables, in the descriptions of their tools: Ollama makes the tool schemas of the docstrings
for tool, columns in [(query_days, DAY_COLUMNS), (query_hours, HOUR_COLUMNS)]:
    tool.__doc__ = (tool.__doc__ or "").replace("{columns}", columns.replace("\n", " "))


def period(start: str, end: str, max_days: int) -> tuple[date, date]:
    first, last = date.fromisoformat(start), date.fromisoformat(end)
    if not archive.FIRST_DAY <= first <= last <= archive.last_forecast_day():
        raise ValueError(f"the period must be within {archive.FIRST_DAY} and {archive.last_forecast_day()}")
    if (last - first).days > max_days:
        raise ValueError(f"the period is too long, at most {max_days} days: split it into several queries")
    return first, last


def run_sql(sql: str, name: str, table: pd.DataFrame, place: places.Place | None = None) -> str:
    """
    The result of a SELECT on a table as JSON rows, after the place of the table. DuckDB runs it in memory
    without access to files or the network, so that a query can read the table only. A result of more than
    MAX_ROWS rows or MAX_RESULT_CHARS characters is refused, for the query to aggregate it.
    """
    if not sql.lstrip().lower().startswith(("select", "with")) or ";" in sql.rstrip().rstrip(";"):
        raise ValueError("only a single SELECT query is allowed")
    if ARBITRARY_ROW.search(sql):
        raise ValueError(
            "ANY_VALUE, ARBITRARY, FIRST and LAST take an arbitrary row: aggregate the rows with AVG, SUM, MIN, MAX "
            "or COUNT, and get the day of the highest value with arg_max(date, column)"
        )
    with duckdb.connect() as con:
        con.register(name, table)
        con.execute("SET enable_external_access = false")
        try:
            result = con.execute(sql.rstrip().rstrip(";")).df()
        except duckdb.Error as error:
            advice = "Add it to GROUP BY, or aggregate it with AVG, SUM, MIN, MAX or COUNT."
            raise ValueError(ANY_VALUE_HINT.sub(advice, str(error))) from None
    if len(result) > MAX_ROWS:
        raise ValueError(
            f"the query gives {len(result)} rows, more than {MAX_ROWS}: aggregate them in SQL, one row for each "
            "month of the year with GROUP BY month(date), for each year with GROUP BY year(date). A LIMIT would "
            "keep some of them only."
        )
    # Values to one decimal, as the answer writes them: a model reads numbers as text and rounds them badly,
    # 7.962 to 7.9
    rows = json.loads(result.to_json(orient="records", date_format="iso", double_precision=1))
    found = {"place": described(place)} if place is not None else {}
    note = f"{len(result)} row{'' if len(result) == 1 else 's'}"
    if result.empty:
        note += ": no row matches the conditions of the query, check them"
    text = json.dumps({**found, "rows": rows, "note": note}, ensure_ascii=False)
    if len(text) > MAX_RESULT_CHARS:
        raise ValueError(f"the result is too long, {len(text)} characters: select only the columns the answer needs")
    return text


def readings(token: str) -> set[float]:
    """
    The values a number can be read as: 1.234,5 the Italian way and 1,234.5 the English way, a lone comma or
    dot as decimal or thousands separator.
    """
    sign = -1 if token[0] in "-−" else 1
    digits = token.lstrip("-−").rstrip(".,")
    candidates = {digits.replace(".", "").replace(",", "."), digits.replace(",", "")}
    if "," in digits and "." not in digits:
        candidates.add(digits.replace(",", "."))
    values = set()
    for candidate in candidates:
        try:
            values.add(sign * float(candidate))
        except ValueError:
            continue
    return values


def numbers(text: str) -> set[float]:
    """Every number of a text, in each way it can be read."""
    return {value for token in NUMBER.findall(text) for value in readings(token)}


def unverified(text: str, question: str, steps: "list[Step]") -> list[str]:
    """
    The numbers of an answer that neither the question nor the data the agent read gave, rounded to one
    decimal or to units: the model computed or made them up, so they are to be checked. Zero, as the minutes
    of 17:00, passes.
    """
    given = numbers(question) | {value for step in steps for value in numbers(json.dumps(step.arguments) + step.result)}

    def read(value: float) -> bool:
        tolerance = 0.5 if value == round(value) else 0.05
        return value == 0 or any(abs(value - known) <= tolerance + 1e-9 for known in given)

    return [token for token in NUMBER.findall(text) if not any(read(value) for value in readings(token))]


@dataclass
class Step:
    """A tool call of the agent."""

    tool: str
    arguments: dict[str, Any]
    result: str
    seconds: float
    error: bool = False


@dataclass
class Answer:
    text: str
    model: str
    steps: list[Step] = field(default_factory=list)
    seconds: float = 0.0
    prompt_tokens: int = 0
    output_tokens: int = 0
    # Numbers of the answer that the data did not give
    unverified: list[str] = field(default_factory=list)


def run_tool(name: str, arguments: dict[str, Any]) -> Step:
    """Run a tool call; an error is given back to the model as its result, for it to fix the call."""
    start = time.perf_counter()
    try:
        if name not in TOOLS:
            raise ValueError(f"no tool {name!r}, the tools are {', '.join(TOOLS)}")
        result, error = TOOLS[name](**arguments), False
    except Exception as exception:  # every failure goes back to the model
        result, error = f"Error: {exception}", True
    return Step(name, dict(arguments), result, time.perf_counter() - start, error)


def conversation_tokens(messages: list[Any]) -> int:
    """An upper estimate of the tokens of a conversation, with the descriptions of the tools."""
    chars = 0
    for message in messages:
        if isinstance(message, dict):
            chars += len(message.get("content") or "")
        else:
            calls = message.tool_calls or []
            chars += len(message.content or "") + sum(len(json.dumps(call.function.arguments)) for call in calls)
    return TOOL_TOKENS + int(chars / CHARS_PER_TOKEN)


def ask(
    question: str,
    history: list[dict[str, str]] | None = None,
    model: str = MODEL,
    think: bool | str = False,
    client: Any = None,
    on_step: Callable[[Step], None] | None = None,
    today: date | None = None,
) -> Answer:
    """
    Answer a question, after the earlier turns of the conversation (their text, as user and assistant
    messages). Thinking makes the model reason before each turn: slower, sometimes better. gpt-oss always
    reasons, at the level think gives ("low", "medium" or "high").
    """
    client = client or ollama.Client(host=HOST)
    messages: list[Any] = [
        {"role": "system", "content": system_prompt(today)},
        *(history or []),
        {"role": "user", "content": question},
    ]
    answer = Answer("", model)
    start = time.perf_counter()
    for _ in range(MAX_STEPS):
        if conversation_tokens(messages) > CONTEXT - CONTEXT_RESERVE:
            answer.text = NOT_IN_CONTEXT
            break
        response = client.chat(
            model=model,
            messages=messages,
            tools=list(TOOLS.values()),
            think=think,
            options={"temperature": 0, "num_ctx": CONTEXT},
            keep_alive=KEEP_ALIVE,
        )
        answer.prompt_tokens += response.prompt_eval_count or 0
        answer.output_tokens += response.eval_count or 0
        messages.append(response.message)
        calls = response.message.tool_calls or []
        if not calls:
            answer.text = (response.message.content or "").strip()
            answer.unverified = unverified(answer.text, question, answer.steps)
            break
        for call in calls:
            step = run_tool(call.function.name, call.function.arguments)
            answer.steps.append(step)
            messages.append({"role": "tool", "content": step.result, "tool_name": step.tool})
            if on_step is not None:
                on_step(step)
    else:
        answer.text = f"No answer within {MAX_STEPS} steps: try a simpler question."
    answer.seconds = time.perf_counter() - start
    return answer


def available_models(client: Any = None) -> list[str]:
    """The models Ollama has downloaded; raises when Ollama does not answer within a few seconds."""
    client = client or ollama.Client(host=HOST, timeout=5)
    return sorted(model.model for model in client.list().models)


if __name__ == "__main__":
    question = " ".join(sys.argv[1:]) or "Che tempo farà domani a Milano?"
    answer = ask(question, on_step=lambda step: print("->", step.tool, json.dumps(step.arguments, ensure_ascii=False)))
    print(f"\n{answer.text}\n\n{answer.seconds:.1f} s, {len(answer.steps)} tool calls, {answer.output_tokens} tokens")
    if answer.unverified:
        print(f"Not in the data: {', '.join(answer.unverified)}")
