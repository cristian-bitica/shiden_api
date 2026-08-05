"""Small shared date utilities.

``date_range`` previously existed as four identical private copies in the
BNR, ECB and ENTSO-E ingesters and in dim_date.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import date, timedelta


def date_range(start: date, end: date) -> Iterator[date]:
    """Yield every date from ``start`` to ``end`` inclusive."""
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def date_to_id(d: date) -> int:
    """YYYYMMDD integer key used across Silver and Gold tables."""
    return d.year * 10_000 + d.month * 100 + d.day
