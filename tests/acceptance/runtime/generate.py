"""Acceptance entrypoint generator.

Turns a ``gherkin-parser`` IR JSON file into a generated, executable pytest
module bound to a step-handler registry. Keeps generated acceptance tests
physically separate from tests/unit: the output lives wherever the caller
points it (by convention, under ``./tmp/``), never under ``tests/unit``,
and is never imported by hand -- only produced by this generator and then
run by pytest as its own step.

Usage:
    python -m tests.acceptance.runtime.generate <ir.json> <steps-module> <output.py>

``<steps-module>`` is the dotted import path of a module exposing a
module-level ``registry: StepRegistry`` (e.g.
``tests.acceptance.steps.daily_job_step_isolation_steps``).
"""

from __future__ import annotations

import sys
from pathlib import Path

from tests.acceptance.runtime.engine import Scenario, expand_scenarios, load_ir

_MODULE_TEMPLATE = '''"""Generated acceptance tests -- DO NOT EDIT BY HAND.

Generated from {ir_path} by tests/acceptance/runtime/generate.py against
step handlers in {steps_module}. Edit the .feature file or the step
handlers instead, then regenerate.
"""

from __future__ import annotations

from {steps_module} import registry
from tests.acceptance.runtime.engine import Scenario, Step, run_scenario

{test_functions}
'''

_TEST_FUNCTION_TEMPLATE = '''def test_{safe_name}() -> None:
    scenario = Scenario(
        name={name!r},
        steps=[
{steps}
        ],
    )
    result = run_scenario(scenario, registry)
    assert result.passed, f"step {{result.failed_step}} failed: {{result.error!r}}"
'''


def _safe_name(name: str) -> str:
    return "".join(c if c.isalnum() else "_" for c in name)


def _render_test_function(scenario: Scenario) -> str:
    steps_src = ",\n".join(
        f"            Step(keyword={step.keyword!r}, text={step.text!r})"
        for step in scenario.steps
    )
    return _TEST_FUNCTION_TEMPLATE.format(
        safe_name=_safe_name(scenario.name),
        name=scenario.name,
        steps=steps_src,
    )


def generate(ir_path: str, steps_module: str, output_path: str) -> None:
    ir = load_ir(ir_path)
    scenarios = expand_scenarios(ir)
    if not scenarios:
        raise ValueError(f"{ir_path}: no scenarios to generate tests for")

    test_functions = "\n\n".join(_render_test_function(s) for s in scenarios)
    output = _MODULE_TEMPLATE.format(
        ir_path=ir_path,
        steps_module=steps_module,
        test_functions=test_functions,
    )

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(output, encoding="utf-8")


def main(argv: list[str] | None = None) -> None:
    args = sys.argv[1:] if argv is None else argv
    if len(args) != 3:
        raise SystemExit("usage: generate.py <ir.json> <steps-module> <output.py>")
    generate(*args)


if __name__ == "__main__":
    main()
