"""DST-aware delivery-day time axis.

The single source of truth for "what intervals exist on a given local
delivery day, and what instant is each one". Pure stdlib, no Spark, so the
DST rules are unit-testable in isolation and every processor derives its
time columns from the same code instead of reimplementing them.

Why this module exists
----------------------
A power delivery day is a *local* day, and on the two DST changeovers a
local day is not 24 hours long:

    spring forward   23 h   92 quarter-hour intervals
    normal           24 h   96
    fall back        25 h  100

OPCOM numbers its intervals by *elapsed slot within the local day*, not by
clock position. Its own summary block proves it -- the 08:00-20:00 peak
window is published as intervals 33-80 normally, 29-76 on 2026-03-29 and
37-84 on 2025-10-26. Deriving a clock time by treating the interval number
as a fixed slot (``time_id = interval - 1``) is therefore correct on 363
days a year and an hour out on the other two, in opposite directions.

The consequences of getting it wrong are not cosmetic: on the fall-back day
local 03:00 occurs twice, so ``(local_date, local_time)`` is not unique and
any table keyed on it silently collapses two distinct hours into one. Only
the UTC instant is a safe key. Local time is an attribute.

Iteration therefore happens in UTC -- stepping 15 minutes at a time from
one local midnight to the next -- and local wall clock is derived from it.
That direction is what makes the 92/96/100 counts fall out naturally rather
than needing special cases.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

INTERVAL_MINUTES = 15
INTERVALS_PER_HOUR = 60 // INTERVAL_MINUTES

# OPCOM's official peak block: 08:00-20:00 local. Other markets differ, so
# processors pass the market's own window rather than relying on these.
DEFAULT_PEAK_START_HOUR = 8
DEFAULT_PEAK_END_HOUR = 20

UTC = timezone.utc


@dataclass(frozen=True)
class Interval:
    """One settlement interval of a local delivery day."""

    timestamp_utc: datetime
    """The instant. Unique, monotonic, and the only safe key."""

    local_timestamp: datetime
    """Wall clock in the market's zone. Ambiguous on the fall-back hour."""

    interval_of_day: int
    """1-based elapsed slot within the local day. Matches OPCOM numbering."""

    local_time_id: int
    """Clock position 0-95 (hour * 4 + quarter). NOT unique on fall-back days."""

    local_hour: int
    quarter_of_hour: int
    time_label: str
    utc_offset_minutes: int
    is_dst: bool
    is_repeated_hour: bool
    """True for the second pass through a repeated local hour (fall back)."""

    is_hour_start: bool
    is_peak: bool

    @property
    def date_id(self) -> int:
        """YYYYMMDD of the *local* delivery date this interval belongs to."""
        d = self.local_timestamp
        return d.year * 10_000 + d.month * 100 + d.day


def local_midnight_utc(local_date: date, tz: ZoneInfo) -> datetime:
    """Return the UTC instant at which ``local_date`` begins in ``tz``.

    ``fold=0`` selects the earlier occurrence should midnight itself ever be
    ambiguous. Romania transitions at 03:00/04:00 so this never bites here,
    but the rule keeps the function honest for zones that shift at midnight.
    """
    naive_midnight = datetime.combine(local_date, time.min)
    aware = naive_midnight.replace(tzinfo=tz, fold=0)
    return aware.astimezone(UTC)


def day_length_hours(local_date: date, tz: ZoneInfo) -> float:
    """Length of the local day in hours: 23, 24 or 25 in a DST zone."""
    start = local_midnight_utc(local_date, tz)
    end = local_midnight_utc(local_date + timedelta(days=1), tz)
    return (end - start).total_seconds() / 3600


def day_intervals(
    local_date: date,
    tz: ZoneInfo,
    peak_start_hour: int = DEFAULT_PEAK_START_HOUR,
    peak_end_hour: int = DEFAULT_PEAK_END_HOUR,
) -> list[Interval]:
    """Every settlement interval of one local delivery day, in order.

    Returns 92, 96 or 100 intervals depending on DST, matching what OPCOM
    publishes for that date.
    """
    start_utc = local_midnight_utc(local_date, tz)
    end_utc = local_midnight_utc(local_date + timedelta(days=1), tz)
    step = timedelta(minutes=INTERVAL_MINUTES)

    intervals: list[Interval] = []
    seen_local_time_ids: set[int] = set()

    current = start_utc
    index = 1
    while current < end_utc:
        local = current.astimezone(tz)
        local_time_id = (
            local.hour * INTERVALS_PER_HOUR + local.minute // INTERVAL_MINUTES
        )

        offset = local.utcoffset() or timedelta(0)
        dst_offset = local.dst() or timedelta(0)

        intervals.append(
            Interval(
                timestamp_utc=current,
                local_timestamp=local,
                interval_of_day=index,
                local_time_id=local_time_id,
                local_hour=local.hour,
                quarter_of_hour=local.minute // INTERVAL_MINUTES,
                time_label=f"{local.hour:02d}:{local.minute:02d}",
                utc_offset_minutes=int(offset.total_seconds() // 60),
                is_dst=dst_offset != timedelta(0),
                # A clock position already seen earlier in the same local day
                # is the repeated hour. Derived from observation rather than
                # from ``fold`` so it stays correct regardless of how the
                # platform populates fold on astimezone().
                is_repeated_hour=local_time_id in seen_local_time_ids,
                is_hour_start=local.minute == 0,
                is_peak=peak_start_hour <= local.hour < peak_end_hour,
            )
        )
        seen_local_time_ids.add(local_time_id)
        current += step
        index += 1

    return intervals


def peak_interval_range(intervals: list[Interval]) -> tuple[int, int]:
    """First and last ``interval_of_day`` flagged peak.

    Exists to be checked against OPCOM's published peak label, which is the
    only external confirmation available that the axis is aligned with the
    market operator's own numbering.
    """
    peaks = [iv.interval_of_day for iv in intervals if iv.is_peak]
    if not peaks:
        raise ValueError("no peak intervals in this day")
    return peaks[0], peaks[-1]
