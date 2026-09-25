"""Acceptance pipeline runtime.

Expands a ``gherkin-parser`` IR JSON file into concrete scenarios (Scenario
Outline examples substituted in) and runs each scenario's steps against a
:class:`StepRegistry` of regex-based step handlers.

This runtime never imports application code itself -- only the IR and a
step-handler module supply behavior -- so it stays reusable across
features. It is invoked by ``tests/acceptance/runtime/generate.py``
(the entrypoint generator), not by pytest collecting this module directly.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Callable


@dataclass(frozen=True)
class Step:
    keyword: str
    text: str


@dataclass(frozen=True)
class Scenario:
    name: str
    steps: list[Step]


StepHandler = Callable[..., None]


class StepRegistry:
    """Regex-based step handler registry.

    One handler per distinct step *shape* (regex captures the parameters),
    per the constitution's default of regex-based parameter extraction --
    not one handler per literal wording.
    """

    def __init__(self) -> None:
        self._patterns: list[tuple[re.Pattern[str], StepHandler]] = []

    def step(self, pattern: str) -> Callable[[StepHandler], StepHandler]:
        compiled = re.compile(pattern)

        def register(fn: StepHandler) -> StepHandler:
            self._patterns.append((compiled, fn))
            return fn

        return register

    def resolve(self, text: str) -> tuple[StepHandler, dict[str, str]]:
        for pattern, fn in self._patterns:
            match = pattern.match(text)
            if match:
                return fn, match.groupdict()
        raise LookupError(f"No step handler registered for: {text!r}")


def load_ir(path: str | Path) -> dict:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def expand_scenarios(ir: dict) -> list[Scenario]:
    """Expand each scenario's Examples into concrete, substituted scenarios.

    A scenario with no examples runs once, unchanged.
    """
    scenarios: list[Scenario] = []
    for raw in ir["scenarios"]:
        examples = raw.get("examples") or [{}]
        multiple = len(examples) > 1
        for row_index, row in enumerate(examples):
            steps = [
                Step(keyword=s["keyword"], text=_substitute(s["text"], row))
                for s in raw["steps"]
            ]
            name = f"{raw['name']}[{row_index}]" if multiple else raw["name"]
            scenarios.append(Scenario(name=name, steps=steps))
    return scenarios


def _substitute(text: str, row: dict[str, str]) -> str:
    for key, value in row.items():
        text = text.replace(f"<{key}>", value)
    return text


@dataclass
class ScenarioResult:
    scenario: Scenario
    passed: bool
    failed_step: Step | None = None
    error: BaseException | None = None


def run_scenario(scenario: Scenario, registry: StepRegistry) -> ScenarioResult:
    """Run every step of ``scenario`` in order against ``registry``.

    Step handlers signal a scenario failure by raising ``AssertionError``;
    any other exception is a defect in the step handler itself and is left
    to propagate.
    """
    context: dict[str, object] = {}
    for step in scenario.steps:
        handler, kwargs = registry.resolve(step.text)
        try:
            handler(context, **kwargs)
        except AssertionError as exc:
            return ScenarioResult(scenario, passed=False, failed_step=step, error=exc)
    return ScenarioResult(scenario, passed=True)
