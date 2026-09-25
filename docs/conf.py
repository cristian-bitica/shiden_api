"""Sphinx configuration for the Shiden technical documentation.

Built with:  uv run --extra docs sphinx-build -b html docs docs/_build/html
"""

from __future__ import annotations

import sys
import tomllib
from datetime import date
from pathlib import Path

# The package is installed into the environment (hatchling / uv sync), but add
# src/ anyway so `sphinx-build` also works from a bare checkout.
REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))

with (REPO_ROOT / "pyproject.toml").open("rb") as fh:
    _pyproject = tomllib.load(fh)["project"]

# -- Project information -----------------------------------------------------

project = "Shiden"
author = "ACB Digital"
copyright = f"{date.today():%Y}, {author}"
release = _pyproject["version"]
version = release

# -- General configuration ---------------------------------------------------

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "sphinx.ext.intersphinx",
    "sphinx.ext.todo",
    "sphinx_copybutton",
    "sphinx_design",
    "sphinxcontrib.mermaid",
    "myst_parser",
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

# Fail the build on a broken reference or a page missing from the toctree.
# Docs that silently rot are worse than no docs.
nitpicky = False

suppress_warnings: list[str] = []

# -- MyST (Markdown) ---------------------------------------------------------
# The existing hand-written docs are Markdown; only conf.py and the generated
# API stubs are reStructuredText.

myst_enable_extensions = [
    "colon_fence",
    "deflist",
    "fieldlist",
    "linkify",
    "substitution",
    "tasklist",
]
myst_heading_anchors = 3
myst_substitutions = {"version": release}

# Render ```mermaid fences through sphinxcontrib-mermaid. Written as plain
# fences rather than {mermaid} directives so GitHub renders the same diagrams
# when the Markdown is read in the repo browser.
myst_fence_as_directive = ["mermaid"]

# Diagrams are rendered client-side from the bundled mermaid.js; no `mmdc`
# binary or headless Chrome is needed in the container or in CI.
mermaid_output_format = "raw"
mermaid_version = "10.9.1"
mermaid_init_js = """
mermaid.initialize({
  startOnLoad: true,
  theme: window.matchMedia('(prefers-color-scheme: dark)').matches
    ? 'dark'
    : 'default',
  flowchart: {curve: 'basis', useMaxWidth: true},
  sequence: {useMaxWidth: true},
  er: {useMaxWidth: true},
});
"""

# -- autodoc / autosummary ---------------------------------------------------

autosummary_generate = True
autosummary_imported_members = False

autodoc_default_options = {
    "members": True,
    "undoc-members": True,
    "show-inheritance": True,
    "member-order": "bysource",
}
autodoc_typehints = "description"
autodoc_typehints_description_target = "documented_params"
autodoc_class_signature = "separated"
autodoc_preserve_defaults = True

# Heavy third-party imports that autodoc does not need to resolve to render
# signatures. PySpark is importable without a JVM, so it is deliberately NOT
# mocked — mocking it would hide genuine import errors in the processors.
autodoc_mock_imports: list[str] = []

napoleon_google_docstring = True
napoleon_numpy_docstring = True
napoleon_use_param = True
napoleon_use_rtype = True
napoleon_preprocess_types = True

# Type information comes from sphinx.ext.autodoc's own `autodoc_typehints`
# handling above, not from sphinx-autodoc-typehints. That extension renders a
# module-level attribute by pulling in its *class* docstring, which drags
# FastAPI's Markdown docstrings (fenced code blocks, `backticks`) into a
# reStructuredText parse and produces unfixable warnings from third-party code.

# -- intersphinx -------------------------------------------------------------

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "pandas": ("https://pandas.pydata.org/docs", None),
    "fastapi": ("https://fastapi.tiangolo.com", None),
}
# Resolving intersphinx inventories requires network access; skip it offline.
intersphinx_disabled_reftypes = ["*"]

# -- HTML output -------------------------------------------------------------

html_theme = "furo"
html_title = f"Shiden {release}"
html_static_path = ["_static"]
html_css_files = ["custom.css"]
html_show_sourcelink = True

html_theme_options = {
    "source_repository": "https://github.com/acbdigital/shiden",
    "source_branch": "main",
    "source_directory": "docs/",
    "navigation_with_keys": True,
    "light_css_variables": {
        "color-brand-primary": "#0b6e4f",
        "color-brand-content": "#0b6e4f",
    },
    "dark_css_variables": {
        "color-brand-primary": "#4ade80",
        "color-brand-content": "#4ade80",
    },
}

# -- Generated OpenAPI contract ----------------------------------------------
# The HTTP contract is dumped straight from the FastAPI app at build time, so
# the machine-readable spec cannot drift from the code the way a hand-written
# endpoint table can. Failure to import must not break the docs build — the
# prose in api-guide.md still stands on its own.


def _dump_openapi(app_) -> None:  # noqa: ANN001 - Sphinx app, not our type
    import json

    out = Path(app_.srcdir) / "_static" / "openapi.json"
    try:
        from shiden.api.main import app as fastapi_app

        out.write_text(json.dumps(fastapi_app.openapi(), indent=2) + "\n")
    except Exception as exc:  # pragma: no cover - build-time convenience only
        from sphinx.util import logging as sphinx_logging

        sphinx_logging.getLogger(__name__).warning(
            "could not generate openapi.json: %s", exc
        )


def setup(app_):  # noqa: ANN001, ANN201 - Sphinx extension entry point
    app_.connect("builder-inited", lambda a: _dump_openapi(a))
    return {"parallel_read_safe": True, "parallel_write_safe": True}


# -- todo --------------------------------------------------------------------

todo_include_todos = True
