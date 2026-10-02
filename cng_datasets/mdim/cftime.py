"""
CF-convention time decoding for N-D cubes (issue #181).

GDAL's multidimensional API hands back a time coordinate as raw offsets plus a
``units`` string ("days since 2015-01-01 12:00:00") and a ``calendar``
attribute; it does not decode them. Climate cubes routinely use calendars that
are not the Gregorian one — CMIP models run on ``noleap`` and ``360_day`` — so
numpy's datetime64 alone is not enough, and getting a calendar wrong shifts
every date by days per decade without any error.

Decoded times are returned as (year, month, day) integer arrays, which are exact
for every supported calendar, plus ``dates`` (numpy ``datetime64[D]``) when
every date exists in the Gregorian calendar. That holds for the Gregorian family
and ``noleap``, but not for ``all_leap`` (29 February every year) or ``360_day``
(30 February), which get ``dates=None``.
"""

import re
from typing import NamedTuple, Optional

import numpy as np

_UNIT_SECONDS = {
    "day": 86400, "days": 86400, "d": 86400,
    "hour": 3600, "hours": 3600, "hr": 3600, "h": 3600,
    "minute": 60, "minutes": 60, "min": 60,
    "second": 1, "seconds": 1, "sec": 1, "s": 1,
}

_UNITS_RE = re.compile(
    r"^\s*(?P<unit>[a-zA-Z]+)\s+since\s+"
    r"(?P<y>-?\d{1,4})-(?P<m>\d{1,2})-(?P<d>\d{1,2})"
    r"(?:[ T](?P<H>\d{1,2}):(?P<M>\d{1,2})(?::(?P<S>\d{1,2}(?:\.\d*)?))?)?"
    r"\s*(?:Z|UTC|[+-]00:?00)?\s*$"
)

GREGORIAN = ("standard", "gregorian", "proleptic_gregorian")
NOLEAP = ("noleap", "365_day")
ALL_LEAP = ("all_leap", "366_day")
DAY_360 = ("360_day",)

# Cumulative days before each month, for the fixed-length-year calendars.
_CUM_NOLEAP = np.cumsum([0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30])
_CUM_LEAP = np.cumsum([0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30])

# Where the "standard"/"gregorian" calendar stops being proleptic Gregorian.
_GREGORIAN_START = np.datetime64("1582-10-15", "D")


class DecodedTime(NamedTuple):
    year: np.ndarray
    month: np.ndarray
    day: np.ndarray
    dates: Optional[np.ndarray]   # datetime64[D], or None for all_leap / 360_day
    calendar: str


def parse_units(units: str):
    """(seconds per unit, (y, m, d), seconds into the reference day)."""
    if not units:
        raise ValueError("time coordinate has no units; cannot decode it")
    m = _UNITS_RE.match(units)
    if not m:
        raise ValueError(f"unrecognised CF time units {units!r} "
                         f"(expected '<unit> since YYYY-MM-DD[ HH:MM[:SS]]')")
    unit = m.group("unit").lower()
    if unit not in _UNIT_SECONDS:
        raise ValueError(f"unsupported CF time unit {unit!r} in {units!r} "
                         f"(months and years are ambiguous in CF and are refused)")
    ref = (int(m.group("y")), int(m.group("m")), int(m.group("d")))
    secs = (int(m.group("H") or 0) * 3600 + int(m.group("M") or 0) * 60
            + float(m.group("S") or 0))
    return _UNIT_SECONDS[unit], ref, secs


def _fixed_year(offset_seconds, ref, ref_secs, year_len, cum):
    """Decode on a calendar whose every year has *year_len* days."""
    y, m, d = ref
    ref_day = y * year_len + (cum[m - 1] if year_len != 360 else 30 * (m - 1)) + (d - 1)
    day_number = np.floor((ref_day * 86400.0 + ref_secs + offset_seconds) / 86400.0).astype(np.int64)
    year = np.floor_divide(day_number, year_len)
    doy = day_number - year * year_len
    if year_len == 360:
        month = doy // 30 + 1
        day = doy % 30 + 1
    else:
        month = np.searchsorted(cum, doy, side="right")
        day = doy - cum[month - 1] + 1
    return year.astype(np.int32), month.astype(np.int32), day.astype(np.int32)


def decode_cf_time(values, units: str, calendar: Optional[str] = None) -> DecodedTime:
    """Decode CF time offsets *values* on *calendar* (default ``standard``)."""
    cal = (calendar or "standard").strip().lower()
    unit_seconds, ref, ref_secs = parse_units(units)
    offsets = np.asarray(values, dtype=np.float64) * unit_seconds

    if cal in GREGORIAN:
        y, m, d = ref
        base = np.datetime64(f"{y:04d}-{m:02d}-{d:02d}", "s") + np.timedelta64(int(ref_secs), "s")
        stamps = base + np.round(offsets).astype("timedelta64[s]")
        dates = stamps.astype("datetime64[D]")
        if cal != "proleptic_gregorian" and len(dates) and dates.min() < _GREGORIAN_START:
            raise ValueError(
                f"calendar {cal!r} switches to Julian before 1582-10-15, and this "
                f"time axis reaches {dates.min()}; that is not supported")
        year = dates.astype("datetime64[Y]").astype(np.int64) + 1970
        month = dates.astype("datetime64[M]").astype(np.int64) % 12 + 1
        day = (dates - dates.astype("datetime64[M]")).astype(np.int64) + 1
        return DecodedTime(year.astype(np.int32), month.astype(np.int32),
                           day.astype(np.int32), dates, cal)

    if cal in NOLEAP:
        year, month, day = _fixed_year(offsets, ref, ref_secs, 365, _CUM_NOLEAP)
        # Every noleap date is a real Gregorian date, so it has a DATE.
        dates = ((year.astype(np.int64) - 1970).astype("datetime64[Y]").astype("datetime64[M]")
                 + (month.astype(np.int64) - 1)).astype("datetime64[D]") + (day.astype(np.int64) - 1)
        return DecodedTime(year, month, day, dates, cal)
    if cal in ALL_LEAP:
        year, month, day = _fixed_year(offsets, ref, ref_secs, 366, _CUM_LEAP)
        return DecodedTime(year, month, day, None, cal)
    if cal in DAY_360:
        year, month, day = _fixed_year(offsets, ref, ref_secs, 360, None)
        return DecodedTime(year, month, day, None, cal)
    raise ValueError(f"unsupported CF calendar {calendar!r}")
