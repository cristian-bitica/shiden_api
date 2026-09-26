#!/usr/bin/env python3
"""Executable QA script for daily_job_step_isolation.qa.md.

Black-box only: drives the project's CLI UI
(``uv run python -m shiden.scheduler.jobs --job <job>``) via subprocess and
inspects its exit code and captured stdout/stderr. No internal project API
is imported or called.

Usage: uv run python tests/acceptance/qa/daily_job_step_isolation_qa.py
"""

from __future__ import annotations

import os
import shutil
import stat
import subprocess
import sys
import tempfile

JOBS = [
    "run_opcom_pzu_daily",
    "run_weather_daily",
    "run_entsoe_daily",
    "run_fx_rates_daily",
    "run_silver_daily",
    "run_gold_daily",
]


def run_job(job: str) -> tuple[int, str]:
    landing_dir = tempfile.mkdtemp(prefix=f"qa-landing-{job}-")
    os.chmod(landing_dir, stat.S_IRUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP)
    try:
        env = dict(os.environ)
        env["LANDING_BASE_PATH"] = landing_dir
        proc = subprocess.run(
            ["uv", "run", "python", "-m", "shiden.scheduler.jobs", "--job", job],
            env=env,
            capture_output=True,
            text=True,
            timeout=120,
        )
        output = proc.stdout + proc.stderr
        return proc.returncode, output
    finally:
        os.chmod(landing_dir, stat.S_IRWXU)
        shutil.rmtree(landing_dir, ignore_errors=True)


def check(job: str, exit_code: int, output: str) -> list[str]:
    failures = []
    if exit_code != 0:
        failures.append(f"exit code {exit_code} != 0")
    if " ERROR " not in output:
        failures.append("no ERROR line in captured output")
    if "pipeline complete" not in output:
        failures.append("no '... pipeline complete ...' line in captured output")
    return failures


def main() -> int:
    overall_ok = True
    for job in JOBS:
        exit_code, output = run_job(job)
        failures = check(job, exit_code, output)
        status = "PASS" if not failures else "FAIL"
        print(f"[{status}] {job}")
        for failure in failures:
            print(f"    - {failure}")
        if failures:
            overall_ok = False
            print("    --- captured output ---")
            for line in output.splitlines():
                print(f"    {line}")
    return 0 if overall_ok else 1


if __name__ == "__main__":
    sys.exit(main())
