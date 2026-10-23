"""
Measure the wall time and peak memory of the ingestion on real ERA5 data.

Each run processes the same days into a fresh temporary output directory, in a
subprocess, and appends its measures to benchmarks/results.jsonl:

    uv run python benchmarks/ingestion_benchmark.py --label "netcdf, 4 workers" -- --workers 4

The pipeline of another revision can be measured by pointing --src to its src directory,
e.g. a git worktree of an older commit: git worktree add ../baseline <commit>.
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from datetime import datetime

import psutil
import yaml

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_KEYS = ("output_path", "query_output_path", "h3_lookup_path", "iceberg_warehouse_path")


def parse_arguments():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--label", required=True, help="Name of the scenario in the results")
    parser.add_argument("--src", default=os.path.join(ROOT, "src"), help="Directory containing main_script.py")
    parser.add_argument("--config", default=os.path.join(ROOT, "config", "config.yaml"))
    parser.add_argument("--start_date", default="20220401")
    parser.add_argument("--end_date", default="20220404")
    parser.add_argument("--results", default=os.path.join(ROOT, "benchmarks", "results.jsonl"))
    parser.add_argument("pipeline_args", nargs="*", help="Extra arguments of main_script.py, after --")
    return parser.parse_args()


def run(command, env):
    """Run the command, sampling the memory of its process every 0.2 s; return (seconds, peak GB, exit code)."""
    start = time.perf_counter()
    process = subprocess.Popen(command, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
    monitor = psutil.Process(process.pid)
    peak = 0
    while process.poll() is None:
        try:
            peak = max(peak, sum(p.memory_info().rss for p in [monitor, *monitor.children(recursive=True)]))
        except psutil.NoSuchProcess:
            break
        time.sleep(0.2)
    stderr = process.stderr.read() if process.stderr else ""
    if process.returncode != 0:
        print(stderr[-3000:], file=sys.stderr)
    return time.perf_counter() - start, peak / 1e9, process.returncode


def main():
    args = parse_arguments()
    with open(args.config) as file:
        config = yaml.safe_load(file)

    workdir = tempfile.mkdtemp(prefix="era5-benchmark-")
    try:
        for key in OUTPUT_KEYS:
            config[key] = os.path.join(workdir, key)
        config_path = os.path.join(workdir, "config.yaml")
        with open(config_path, "w") as file:
            yaml.safe_dump(config, file)

        command = [
            sys.executable,
            os.path.join(args.src, "main_script.py"),
            "--config", config_path,
            "--start_date", args.start_date,
            "--end_date", args.end_date,
            "--skip_queries",
            *args.pipeline_args,
        ]  # fmt: skip
        env = {**os.environ, "PYTHONPATH": args.src}
        seconds, peak_gb, exit_code = run(command, env)
    finally:
        shutil.rmtree(workdir, ignore_errors=True)

    days = (datetime.strptime(args.end_date, "%Y%m%d") - datetime.strptime(args.start_date, "%Y%m%d")).days + 1
    variable_days = days * len(config["variables"])
    result = {
        "label": args.label,
        "variable_days": variable_days,
        "seconds": round(seconds, 1),
        "seconds_per_variable_day": round(seconds / variable_days, 2),
        "peak_memory_gb": round(peak_gb, 2),
        "exit_code": exit_code,
        "args": args.pipeline_args,
        "date": datetime.now().isoformat(timespec="seconds"),
    }
    with open(args.results, "a") as file:
        file.write(json.dumps(result) + "\n")
    print(json.dumps(result))
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
