"""Gold layer Delta reader — JVM-free read path for FastAPI.

The write path (Spark batch jobs) produces Delta tables under
``{delta_base_path}/gold/<table_name>``.

The read path (FastAPI, APScheduler callbacks) uses ``deltalake`` (delta-rs)
to read those tables as pandas DataFrames without starting a JVM.  This keeps
API latency in the low-milliseconds range and avoids Spark cold-start cost.

Usage::

    from shiden.processing.gold.reader import read_gold

    df = read_gold("price_hourly", filters=[("market_id", "=", "RO"),
                                             ("date_id", ">=", 20260601),
                                             ("date_id", "<=", 20260630)])
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from shiden.config.settings import settings


def read_gold(
    table_name: str,
    filters: list[tuple[str, str, Any]] | None = None,
) -> pd.DataFrame:
    """Read a Gold Delta table into a pandas DataFrame.

    Parameters
    ----------
    table_name:
        Name of the Gold table without path prefix, e.g. ``"price_hourly"``.
    filters:
        Optional list of ``(column, op, value)`` pushdown predicates accepted
        by ``deltalake.DeltaTable.to_pandas()``.  These are pushed down to
        parquet row-group filtering and partition pruning, keeping memory use
        low for large date ranges.

        Example::

            [("market_id", "=", "RO"), ("date_id", ">=", 20260601)]

    Returns
    -------
    pd.DataFrame
        Full table (or filtered subset) as a pandas DataFrame.

    Raises
    ------
    TableNotFoundError
        If the Gold Delta table does not exist yet (upstream Spark job has not
        run, or an incorrect ``delta_base_path`` is configured).
    """
    from deltalake import DeltaTable
    from deltalake.exceptions import TableNotFoundError  # re-raise with context

    path = f"{settings.delta_base_path}/gold/{table_name}"
    try:
        dt = DeltaTable(path)
    except TableNotFoundError:
        raise TableNotFoundError(
            f"Gold table '{table_name}' not found at {path!r}. "
            "Run the Gold Spark job first."
        )

    return dt.to_pandas(filters=filters)
