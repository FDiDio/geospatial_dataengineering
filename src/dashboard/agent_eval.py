"""
The evaluation of the agent: questions whose answers the code computes from the same data, and the answers
of the agent graded by the numbers they give.

    uv run python src/dashboard/agent_eval.py --model qwen3:8b --model qwen3:4b [--think] [--only milano_hot_days]

An answer passes when the agent read the data of the place, one of the matches of the geocoder, and the answer
gives every expected number within the tolerance of its kind; else it fails for "no data" (a number without a
query), "wrong place" or "wrong value", or "error" when Ollama or the data could not be reached (rerun it).
The expected numbers are computed at each run, at the coordinates the agent queried, so that they follow the
data (a forecast changes every day) and the point: Open-Meteo corrects the temperatures of a grid cell for the
altitude of the point, and a city and its region of the same name can be 15 km apart. Each run appends its
results to benchmarks/agent_eval.jsonl and prints a summary by model.
"""

import argparse
import hashlib
import json
import os
import pickle
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, timedelta

import h3
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dashboard import agent, archive, places, queries, terrain

RESULTS = os.path.join(os.path.dirname(__file__), "..", "..", "benchmarks", "agent_eval.jsonl")
# The tools that read data: an answer must come from one of them
DATA_TOOLS = {"query_days", "query_hours", "cell_altitude"}
# How far, in degrees, the coordinates of a query can be from a match of the geocoder: the agent may round them
NEAR_DEG = 0.05
# Tool errors of the data out of reach, the network down or Open-Meteo limiting the requests: not of the model.
# A case that ends so is asked again after a pause: Open-Meteo counts the requests of a minute.
OUT_OF_REACH = ("urlopen error", "timed out", "Open-Meteo refused", "HTTP Error 5")
RETRY_AFTER_S = 65
# The final days of ERA5 a run loads, kept on disk for the next runs. A run asks decades of hourly data, which
# Open-Meteo counts as about 1,800 of its 10,000 free calls a day (5,000 an hour). ERA5 replaces its preliminary
# days within about three months, so these are loaded each time.
CACHE = os.path.join(os.path.dirname(RESULTS), ".cache")
FINAL_AFTER = timedelta(days=90)


def kept_on_disk(fetch: Callable[..., archive.Weather]) -> Callable[..., archive.Weather]:
    """A fetch of the weather of Open-Meteo, its final ERA5 days kept on disk."""

    def cached(
        latitude: float, longitude: float, start: date, end: date, source: archive.Source = archive.ERA5
    ) -> archive.Weather:
        if source != archive.ERA5 or end > archive.last_day() - FINAL_AFTER:
            return fetch(latitude, longitude, start, end, source)
        key = hashlib.sha256(repr((latitude, longitude, start, end)).encode()).hexdigest()[:24]
        path = os.path.join(CACHE, f"{key}.pkl")
        if os.path.exists(path):
            with open(path, "rb") as file:
                return pickle.load(file)
        weather = fetch(latitude, longitude, start, end, source)
        os.makedirs(CACHE, exist_ok=True)
        with open(path, "wb") as file:
            pickle.dump(weather, file)
        return weather

    return cached


# Tolerances by kind of number: counts exact, values with one decimal, totals within 1 % (rounded to units)
TOLERANCE: dict[str, Callable[[float], float]] = {
    "count": lambda value: 0.0,
    "decimal": lambda value: 0.15,
    "total": lambda value: max(0.15, abs(value) / 100),
}


def days(latitude: float, longitude: float, start: str, end: str) -> pd.DataFrame:
    # The data the agent read, from its cache: Open-Meteo limits the requests of a minute
    return agent.weather(latitude, longitude, date.fromisoformat(start), date.fromisoformat(end)).daily


@dataclass(frozen=True)
class Case:
    id: str
    question: str
    place: str  # the name the geocoder finds the place by
    truth: Callable[[float, float], list[float]]  # the expected numbers, for the coordinates of the place
    kind: str = "decimal"


def summer_means(lat: float, lon: float) -> list[float]:
    return [days(lat, lon, f"{year}-06-01", f"{year}-08-31")["temperature_mean_c"].mean() for year in (2003, 2022)]


def hottest_hour(lat: float, lon: float) -> list[float]:
    hours = agent.weather(lat, lon, date(2022, 7, 1), date(2022, 7, 1)).hourly
    return [float(hours.nlargest(1, "temperature_c")["time"].iloc[0].hour)]


def tomorrow_high(lat: float, lon: float) -> list[float]:
    tomorrow = (date.today() + timedelta(days=1)).isoformat()
    return [days(lat, lon, tomorrow, tomorrow)["temperature_max_c"].iloc[0]]


def annual_means(lat: float, lon: float) -> list[float]:
    daily = days(lat, lon, "2016-01-01", "2025-12-31")
    return daily.groupby(daily["date"].dt.year)["temperature_mean_c"].mean().tolist()


def wettest_month(lat: float, lon: float) -> list[float]:
    daily = days(lat, lon, "2023-01-01", "2023-12-31")
    return [daily.groupby(daily["date"].dt.month)["precipitation_mm"].sum().max()]


def monthly_highs(lat: float, lon: float) -> list[float]:
    daily = days(lat, lon, "2015-01-01", "2025-12-31")
    return daily.groupby(daily["date"].dt.month)["temperature_max_c"].mean().tolist()


def monthly_rain(lat: float, lon: float) -> list[float]:
    """The precipitation of each month of the year, the mean of its totals of 2015 to 2025."""
    daily = days(lat, lon, "2015-01-01", "2025-12-31")
    totals = daily.groupby([daily["date"].dt.year, daily["date"].dt.month])["precipitation_mm"].sum()
    return totals.groupby(level=1).mean().tolist()


CASES = [
    Case("milano_hot_days", "Quanti giorni sopra 30 °C ha avuto Milano nel luglio 2022?", "Milano",
         lambda lat, lon: [(days(lat, lon, "2022-07-01", "2022-07-31")["temperature_max_c"] > 30).sum()], "count"),
    Case("roma_max_2023", "Qual è stata la temperatura massima più alta a Roma nel 2023?", "Roma",
         lambda lat, lon: [days(lat, lon, "2023-01-01", "2023-12-31")["temperature_max_c"].max()]),
    Case("london_rain_oct_2023", "Quanta pioggia è caduta in totale a Londra nell'ottobre 2023?", "London",
         lambda lat, lon: [days(lat, lon, "2023-10-01", "2023-10-31")["precipitation_mm"].sum()], "total"),
    Case("nyc_mean_jan_2024", "Qual è stata la temperatura media di gennaio 2024 a New York?", "New York",
         lambda lat, lon: [days(lat, lon, "2024-01-01", "2024-01-31")["temperature_mean_c"].mean()]),
    Case("moscow_coldest_2022", "Qual è stata la temperatura minima più bassa a Mosca nel 2022?", "Mosca",
         lambda lat, lon: [days(lat, lon, "2022-01-01", "2022-12-31")["temperature_min_c"].min()]),
    Case("madrid_sun_aug_2023", "Quante ore di sole ha avuto Madrid nell'agosto 2023?", "Madrid",
         lambda lat, lon: [days(lat, lon, "2023-08-01", "2023-08-31")["sunshine_h"].sum()], "total"),
    Case("bergen_wet_days_2023", "Quanti giorni con almeno 1 mm di pioggia ha avuto Bergen nel 2023?", "Bergen",
         lambda lat, lon: [(days(lat, lon, "2023-01-01", "2023-12-31")["precipitation_mm"] >= 1).sum()], "count"),
    Case("paris_summers", "Qual è stata la temperatura media dell'estate (da giugno ad agosto) a Parigi nel 2003 e "
         "nel 2022?", "Parigi", summer_means),
    Case("trieste_gust_2023", "Qual è stata la raffica di vento più forte a Trieste nel 2023?", "Trieste",
         lambda lat, lon: [days(lat, lon, "2023-01-01", "2023-12-31")["wind_gusts_max_kmh"].max()], "total"),
    Case("singapore_humidity_2023", "Qual è stata l'umidità relativa media a Singapore nel 2023?", "Singapore",
         lambda lat, lon: [days(lat, lon, "2023-01-01", "2023-12-31")["humidity_mean_pct"].mean()], "total"),
    Case("torino_frost_jan_2024", "Quanti giorni con la minima sotto zero ha avuto Torino nel gennaio 2024?", "Torino",
         lambda lat, lon: [(days(lat, lon, "2024-01-01", "2024-01-31")["temperature_min_c"] < 0).sum()], "count"),
    Case("cortina_altitude", "Qual è l'altitudine media dell'area di Cortina d'Ampezzo?", "Cortina d'Ampezzo",
         lambda lat, lon: [terrain.cell_altitude(h3.latlng_to_cell(lat, lon, queries.PLACE_RESOLUTION)).mean],
         "total"),
    Case("milano_tomorrow", "Che temperatura massima è prevista domani a Milano?", "Milano", tomorrow_high),
    Case("milano_hottest_hour", "A che ora ha fatto più caldo a Milano il 1 luglio 2022?", "Milano", hottest_hour,
         "count"),
    Case("sevilla_35_2022", "Quanti giorni sopra 35 °C ha avuto Siviglia nel 2022?", "Siviglia",
         lambda lat, lon: [(days(lat, lon, "2022-01-01", "2022-12-31")["temperature_max_c"] > 35).sum()], "count"),
    Case("tokyo_rain_2019", "Quanta pioggia è caduta in totale a Tokyo nel 2019?", "Tokyo",
         lambda lat, lon: [days(lat, lon, "2019-01-01", "2019-12-31")["precipitation_mm"].sum()], "total"),
    Case("sydney_mean_2020", "Qual è stata la temperatura media annua di Sydney nel 2020?", "Sydney",
         lambda lat, lon: [days(lat, lon, "2020-01-01", "2020-12-31")["temperature_mean_c"].mean()]),
    Case("valencia_dana", "Quanta pioggia è caduta a Valencia il 29 ottobre 2024?", "Valencia",
         lambda lat, lon: [days(lat, lon, "2024-10-29", "2024-10-29")["precipitation_mm"].iloc[0]], "total"),
    # Long periods: one aggregate over decades, values by year, the largest of the monthly totals
    Case("milano_annual_10y", "Quali sono state le temperature medie annue a Milano negli ultimi 10 anni interi, "
         "dal 2016 al 2025?", "Milano", annual_means),
    Case("roma_normal_1991_2020", "Qual è la temperatura media di Roma nel trentennio 1991-2020?", "Roma",
         lambda lat, lon: [days(lat, lon, "1991-01-01", "2020-12-31")["temperature_mean_c"].mean()]),
    Case("bologna_wettest_month_2023", "Qual è stato il mese più piovoso del 2023 a Bologna, e quanta pioggia è "
         "caduta?", "Bologna", wettest_month, "total"),
    # Monthly climatologies: a mean of all the days of each month, a mean of the totals of each month
    Case("lugano_monthly_highs", "Qual è la temperatura massima media di ciascun mese dell'anno a Lugano, dal 2015 "
         "al 2025?", "Lugano", monthly_highs),
    Case("lugano_monthly_rain", "Quanta pioggia cade in media a Lugano in ciascun mese dell'anno, secondo i dati dal "
         "2015 al 2025?", "Lugano", monthly_rain, "total"),
]  # fmt: skip


def grade(answer: str, expected: list[float], kind: str) -> bool:
    """Whether an answer gives every expected number, within the tolerance of its kind."""
    found = agent.numbers(answer)
    return all(any(abs(value - truth) <= TOLERANCE[kind](truth) + 1e-9 for value in found) for truth in expected)


def queried_point(steps: list[agent.Step]) -> tuple[float, float] | None:
    """The coordinates of the first data the agent read, of the place as the tool found it."""
    step = next((step for step in steps if step.tool in DATA_TOOLS and not step.error), None)
    if step is None:
        return None
    place = json.loads(step.result)["place"]
    return float(place["latitude"]), float(place["longitude"])


def judge(case: Case, answer: agent.Answer, matches: list[places.Place], truths: dict) -> tuple[str | None, list]:
    """Why an answer fails, or None when it passes, and the expected numbers at the point it queried."""
    point = queried_point(answer.steps)
    if point is None:
        out_of_reach = any(step.error and any(text in step.result for text in OUT_OF_REACH) for step in answer.steps)
        return ("error" if out_of_reach else "no data"), []
    if not any(abs(point[0] - m.latitude) <= NEAR_DEG and abs(point[1] - m.longitude) <= NEAR_DEG for m in matches):
        return "wrong place", []
    if (case.id, point) not in truths:
        truths[case.id, point] = [round(float(value), 3) for value in case.truth(*point)]
    expected = truths[case.id, point]
    return (None if grade(answer.text, expected, case.kind) else "wrong value"), expected


def evaluate(cases: list[Case], models: list[str], think: bool | str, record: str = RESULTS) -> list[dict]:
    """The results of the cases for each model, each appended to the record as it comes."""
    # The matches the agent gets too, from the same cache: the geocoder allows one request a second
    matches = {case.place: list(agent.matches(case.place)) for case in cases}
    truths: dict[tuple[str, tuple[float, float]], list[float]] = {}
    results = []
    for model in models:
        for case in cases:
            for attempt in range(2):  # an error again after a pause, as Open-Meteo counts requests by the minute
                if attempt:
                    time.sleep(RETRY_AFTER_S)
                try:
                    answer = agent.ask(case.question, model=model, think=think)
                    failure, expected = judge(case, answer, matches[case.place], truths)
                except Exception as error:  # Ollama or the data out of reach: rerun the case with --only
                    answer, failure, expected = agent.Answer(f"Error: {error}", model), "error", []
                if failure != "error":
                    break
            results.append({
                "time": time.strftime("%Y-%m-%dT%H:%M:%S"), "model": model, "think": think, "case": case.id,
                "passed": failure is None, "failure": failure, "expected": expected, "answer": answer.text,
                "tool_calls": len(answer.steps), "tool_errors": sum(step.error for step in answer.steps),
                "seconds": round(answer.seconds, 1), "output_tokens": answer.output_tokens,
                "unverified": answer.unverified,
                "steps": [{"tool": step.tool, "arguments": step.arguments, "error": step.error,
                           "result": step.result[:300]} for step in answer.steps],
            })  # fmt: skip
            with open(record, "a", encoding="utf-8") as file:
                file.write(json.dumps(results[-1], ensure_ascii=False, default=str) + "\n")
            print(
                f"{model:12} {failure or 'pass':11}  {case.id:24} {answer.seconds:5.1f} s  {answer.text[:80]!r}",
                flush=True,
            )
    return results


def summary(results: list[dict]) -> pd.DataFrame:
    frame = pd.DataFrame(results)
    by_model = frame.groupby(["model", "think"]).agg(
        passed=("passed", "sum"),
        cases=("passed", "size"),
        mean_seconds=("seconds", "mean"),
        mean_tool_calls=("tool_calls", "mean"),
        tool_errors=("tool_errors", "sum"),
    ).assign(accuracy=lambda f: (f["passed"] / f["cases"]).round(2))  # fmt: skip
    failures = pd.crosstab([frame["model"], frame["think"]], frame["failure"])
    return by_model.join(failures).fillna(0)


def latest_results(path: str = RESULTS) -> pd.DataFrame:
    """The latest result of each case, by model and thinking, of all the runs of the record."""
    try:
        frame = pd.read_json(path, lines=True)
    except (FileNotFoundError, ValueError):
        return pd.DataFrame()
    return frame.drop_duplicates(["model", "think", "case"], keep="last") if not frame.empty else frame


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--model", action="append", help="Ollama model, repeatable (default: the agent's)")
    parser.add_argument(
        "--think", nargs="?", const=True, default=False, help="Let the model reason; for gpt-oss: low, medium or high"
    )
    parser.add_argument("--only", action="append", help="Case id, repeatable (default: all)")
    parser.add_argument("--report", action="store_true", help="Print the latest results of the record, run nothing")
    args = parser.parse_args()
    if args.report:
        latest = latest_results()
        print(summary(latest.to_dict("records")).round(1).to_string() if not latest.empty else "No results yet")
        sys.exit()
    cases = [case for case in CASES if not args.only or case.id in args.only]
    # The agent's fetches, and the expected numbers', through the cache on disk
    archive.fetch = kept_on_disk(archive.fetch)
    results = evaluate(cases, args.model or [agent.MODEL], args.think)
    print(f"\n{summary(results).round(1).to_string()}\n\nResults appended to {os.path.normpath(RESULTS)}")
