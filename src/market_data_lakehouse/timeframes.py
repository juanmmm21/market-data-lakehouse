from __future__ import annotations

from datetime import UTC, datetime, timedelta

from market_data_lakehouse.models import Timeframe


def floor_to_timeframe(event_time: datetime, timeframe: Timeframe) -> datetime:
    """Alinea un timestamp al inicio de su vela."""
    if event_time.tzinfo is None:
        raise ValueError("event_time must be timezone-aware")

    utc_time = event_time.astimezone(UTC)
    epoch_seconds = int(utc_time.timestamp())
    bucket_seconds = epoch_seconds - (epoch_seconds % timeframe.seconds)
    return datetime.fromtimestamp(bucket_seconds, tz=UTC)


def candle_close_time(open_time: datetime, timeframe: Timeframe) -> datetime:
    return open_time + timedelta(seconds=timeframe.seconds)
