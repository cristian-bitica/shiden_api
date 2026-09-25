"""Persistent runner-worker adapter for ``gherkin-mutator``.

Implements the newline-delimited-JSON stdin/stdout protocol from
Acceptance-Pipeline-Specification/mutator-spec.md: one JSON job request per
input line, one JSON job response per output line.

``tests.acceptance.runtime.generate`` bakes each scenario's substituted
example values into the generated test source at generation time (see its
module docstring), so the "generate once, replay against mutated IR" model
the spec recommends does not apply here -- this worker regenerates the test
module from the mutated IR on every job instead of reusing the base
generated file.

Usage:
    python -m tests.acceptance.runtime.runner_worker <steps-module>
"""

from __future__ import annotations

import json
import subprocess
import sys
import time
from pathlib import Path

from tests.acceptance.runtime.generate import generate


def _duration_seconds(text: str) -> float | None:
    text = (text or "").strip()
    if not text:
        return None
    if text.endswith("ms"):
        return float(text[:-2]) / 1000
    if text.endswith("s"):
        return float(text[:-1])
    if text.endswith("m"):
        return float(text[:-1]) * 60
    return float(text)


def handle_job(job: dict, steps_module: str) -> dict:
    start = time.monotonic()
    work_dir = Path(job["work_dir"])
    work_dir.mkdir(parents=True, exist_ok=True)
    generated_py = work_dir / "generated_mutant.py"

    try:
        generate(job["feature_json"], steps_module, str(generated_py))
    except Exception as exc:
        return {
            "id": job["id"],
            "outcome": "infrastructure_error",
            "output": "",
            "error": repr(exc),
            "duration": int((time.monotonic() - start) * 1e9),
        }

    try:
        result = subprocess.run(
            [sys.executable, "-m", "pytest", str(generated_py), "-q"],
            capture_output=True,
            text=True,
            timeout=_duration_seconds(job.get("timeout", "")),
        )
    except subprocess.TimeoutExpired as exc:
        return {
            "id": job["id"],
            "outcome": "infrastructure_error",
            "output": (exc.stdout or "") + (exc.stderr or ""),
            "error": "timeout",
            "duration": int((time.monotonic() - start) * 1e9),
        }

    outcome = "test_success" if result.returncode == 0 else "test_failure"
    return {
        "id": job["id"],
        "outcome": outcome,
        "output": result.stdout + result.stderr,
        "error": "",
        "duration": int((time.monotonic() - start) * 1e9),
    }


def main(argv: list[str] | None = None) -> None:
    args = sys.argv[1:] if argv is None else argv
    if len(args) != 1:
        raise SystemExit("usage: runner_worker.py <steps-module>")
    steps_module = args[0]

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        job = json.loads(line)
        response = handle_job(job, steps_module)
        sys.stdout.write(json.dumps(response) + "\n")
        sys.stdout.flush()


if __name__ == "__main__":
    main()
