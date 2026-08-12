"""DST-aware time axis tests.

The load-bearing assertions are the ones checked against OPCOM's own
published peak windows: those are external ground truth, not our arithmetic
restated. If the axis and OPCOM disagree about which interval is 08:00, the
axis is wrong.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest

from shiden.processing.silver.dimensions.dim_datetime import make_rows
from shiden.timeaxis import (
    day_intervals,
    day_length_hours,
    local_midnight_utc,
    peak_interval_range,
)

RO_TZ = ZoneInfo("Europe/Bucharest")

NORMAL_WINTER = date(2026, 2, 1)
NORMAL_SUMMER = date(2026, 6, 2)
SPRING_FORWARD = date(2026, 3, 29)   # 23h — clocks 03:00 → 04:00
FALL_BACK = date(2025, 10, 26)       # 25h — clocks 04:00 → 03:00


class TestDayLength:
    @pytest.mark.parametrize(
        "day,hours",
        [
            (NORMAL_WINTER, 24),
            (NORMAL_SUMMER, 24),
            (SPRING_FORWARD, 23),
            (FALL_BACK, 25),
        ],
    )
    def test_local_day_length(self, day: date, hours: int) -> None:
        assert day_length_hours(day, RO_TZ) == hours

    @pytest.mark.parametrize(
        "day,count",
        [
            (NORMAL_WINTER, 96),
            (NORMAL_SUMMER, 96),
            (SPRING_FORWARD, 92),
            (FALL_BACK, 100),
        ],
    )
    def test_interval_count_matches_opcom(self, day: date, count: int) -> None:
        """OPCOM publishes exactly these counts for these dates."""
        assert len(day_intervals(day, RO_TZ)) == count


class TestPeakWindowAgainstOpcom:
    """OPCOM's summary block names its own peak interval range.

    Normal days read 'ROPEX_DAM_Peak (33-80)'; 2026-03-29 reads (29-76) and
    2025-10-26 reads (37-84). Reproducing those is the strongest available
    check that our interval numbering matches the market operator's.
    """

    @pytest.mark.parametrize(
        "day,expected",
        [
            (NORMAL_WINTER, (33, 80)),
            (NORMAL_SUMMER, (33, 80)),
            (SPRING_FORWARD, (29, 76)),
            (FALL_BACK, (37, 84)),
        ],
    )
    def test_peak_range(self, day: date, expected: tuple[int, int]) -> None:
        assert peak_interval_range(day_intervals(day, RO_TZ)) == expected

    def test_peak_is_always_local_0800_to_2000(self) -> None:
        for day in (NORMAL_WINTER, SPRING_FORWARD, FALL_BACK):
            for iv in day_intervals(day, RO_TZ):
                assert iv.is_peak == (8 <= iv.local_hour < 20)


class TestUtcContinuity:
    @pytest.mark.parametrize(
        "day", [NORMAL_WINTER, NORMAL_SUMMER, SPRING_FORWARD, FALL_BACK]
    )
    def test_utc_instants_are_strictly_increasing(self, day: date) -> None:
        stamps = [iv.timestamp_utc for iv in day_intervals(day, RO_TZ)]
        assert stamps == sorted(stamps)
        assert len(set(stamps)) == len(stamps)

    @pytest.mark.parametrize(
        "day", [NORMAL_WINTER, SPRING_FORWARD, FALL_BACK]
    )
    def test_no_gaps_in_utc(self, day: date) -> None:
        stamps = [iv.timestamp_utc for iv in day_intervals(day, RO_TZ)]
        deltas = {b - a for a, b in zip(stamps, stamps[1:])}
        assert deltas == {timedelta(minutes=15)}

    def test_days_join_end_to_end(self) -> None:
        """The last interval of one day abuts the first of the next."""
        for day in (SPRING_FORWARD, FALL_BACK, NORMAL_WINTER):
            today = day_intervals(day, RO_TZ)
            tomorrow = day_intervals(day + timedelta(days=1), RO_TZ)
            gap = tomorrow[0].timestamp_utc - today[-1].timestamp_utc
            assert gap == timedelta(minutes=15)

    def test_midnight_anchors_to_correct_utc(self) -> None:
        # Winter: EET = UTC+2 → local midnight is 22:00Z the previous day.
        assert local_midnight_utc(NORMAL_WINTER, RO_TZ) == datetime(
            2026, 1, 31, 22, 0, tzinfo=timezone.utc
        )
        # Summer: EEST = UTC+3 → 21:00Z.
        assert local_midnight_utc(NORMAL_SUMMER, RO_TZ) == datetime(
            2026, 6, 1, 21, 0, tzinfo=timezone.utc
        )


class TestSpringForward:
    def test_local_0300_hour_does_not_exist(self) -> None:
        labels = {iv.time_label for iv in day_intervals(SPRING_FORWARD, RO_TZ)}
        assert "03:00" not in labels
        assert "02:45" in labels
        assert "04:00" in labels

    def test_clock_jumps_at_the_transition(self) -> None:
        ivs = day_intervals(SPRING_FORWARD, RO_TZ)
        before = next(iv for iv in ivs if iv.time_label == "02:45")
        after = next(iv for iv in ivs if iv.time_label == "04:00")
        assert after.interval_of_day == before.interval_of_day + 1
        assert after.timestamp_utc - before.timestamp_utc == timedelta(minutes=15)

    def test_delivery_hours_is_23(self) -> None:
        ivs = day_intervals(SPRING_FORWARD, RO_TZ)
        assert len({(iv.local_hour, iv.is_repeated_hour) for iv in ivs}) == 23

    def test_no_interval_flagged_repeated(self) -> None:
        assert not any(iv.is_repeated_hour for iv in day_intervals(SPRING_FORWARD, RO_TZ))


class TestFallBack:
    def test_local_0300_hour_occurs_twice(self) -> None:
        ivs = day_intervals(FALL_BACK, RO_TZ)
        at_0300 = [iv for iv in ivs if iv.time_label == "03:00"]
        assert len(at_0300) == 2
        assert at_0300[0].timestamp_utc != at_0300[1].timestamp_utc

    def test_second_pass_is_flagged_and_offset_shifts(self) -> None:
        ivs = day_intervals(FALL_BACK, RO_TZ)
        first, second = [iv for iv in ivs if iv.time_label == "03:00"]
        assert first.is_repeated_hour is False
        assert second.is_repeated_hour is True
        assert first.utc_offset_minutes == 180   # EEST
        assert second.utc_offset_minutes == 120  # EET
        assert first.is_dst is True
        assert second.is_dst is False

    def test_local_time_id_is_not_unique(self) -> None:
        """Exactly why the UTC instant has to be the key."""
        ivs = day_intervals(FALL_BACK, RO_TZ)
        ids = [iv.local_time_id for iv in ivs]
        assert len(ids) == 100
        assert len(set(ids)) == 96

    def test_delivery_hours_is_25(self) -> None:
        ivs = day_intervals(FALL_BACK, RO_TZ)
        assert len({(iv.local_hour, iv.is_repeated_hour) for iv in ivs}) == 25


class TestIntervalNumbering:
    @pytest.mark.parametrize(
        "day", [NORMAL_WINTER, SPRING_FORWARD, FALL_BACK]
    )
    def test_interval_of_day_is_dense_and_one_based(self, day: date) -> None:
        ivs = day_intervals(day, RO_TZ)
        assert [iv.interval_of_day for iv in ivs] == list(range(1, len(ivs) + 1))

    def test_date_id_is_the_local_delivery_date(self) -> None:
        for day in (NORMAL_WINTER, SPRING_FORWARD, FALL_BACK):
            ivs = day_intervals(day, RO_TZ)
            expected = day.year * 10_000 + day.month * 100 + day.day
            assert {iv.date_id for iv in ivs} == {expected}

    def test_naive_mapping_diverges_only_on_dst_days(self) -> None:
        """time_id = interval - 1 was right 363 days a year, wrong on 2."""
        normal = day_intervals(NORMAL_WINTER, RO_TZ)
        assert all(iv.local_time_id == iv.interval_of_day - 1 for iv in normal)

        for day in (SPRING_FORWARD, FALL_BACK):
            ivs = day_intervals(day, RO_TZ)
            wrong = [iv for iv in ivs if iv.local_time_id != iv.interval_of_day - 1]
            assert len(wrong) > 75, "expected most of the day to be mislabelled"


class TestDimDateTimeRows:
    def test_row_count_matches_axis(self) -> None:
        rows = make_rows("RO", FALL_BACK, FALL_BACK)
        assert len(rows) == 100

    def test_timestamps_are_naive_utc(self) -> None:
        """Session timezone is pinned to UTC; naive values round-trip exactly."""
        for row in make_rows("RO", NORMAL_WINTER, NORMAL_WINTER):
            assert row.timestamp_utc.tzinfo is None

    def test_primary_key_is_unique_across_a_dst_day(self) -> None:
        rows = make_rows("RO", FALL_BACK, FALL_BACK)
        keys = {(r.market_id, r.timestamp_utc) for r in rows}
        assert len(keys) == len(rows)

    def test_alternate_key_is_also_unique(self) -> None:
        rows = make_rows("RO", FALL_BACK, FALL_BACK)
        keys = {(r.market_id, r.date_id, r.interval_of_day) for r in rows}
        assert len(keys) == len(rows)

    def test_multi_day_range(self) -> None:
        rows = make_rows("RO", date(2026, 3, 28), date(2026, 3, 30))
        assert len(rows) == 96 + 92 + 96

    def test_peak_hours_come_from_market_config(self) -> None:
        rows = make_rows("RO", NORMAL_WINTER, NORMAL_WINTER)
        peak_hours = {r.local_hour for r in rows if r.is_peak}
        assert peak_hours == set(range(8, 20))
