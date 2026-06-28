from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from market_data_lakehouse.models import ExchangeId, Timeframe, TradeSide, TradeTick
from market_data_lakehouse.timeframes import candle_close_time, floor_to_timeframe


def _tick(price: str, offset_seconds: int = 0) -> TradeTick:
    base = datetime(2024, 1, 1, 12, 0, 10, tzinfo=UTC) + timedelta(seconds=offset_seconds)
    return TradeTick(
        exchange=ExchangeId.BINANCE,
        symbol="BTCUSDT",
        trade_id=f"id-{offset_seconds}",
        price=Decimal(price),
        quantity=Decimal("1"),
        side=TradeSide.BUY,
        event_time=base,
    )


def test_floor_to_one_minute_bucket() -> None:
    event_time = datetime(2024, 1, 1, 12, 3, 45, tzinfo=UTC)
    bucket = floor_to_timeframe(event_time, Timeframe.M1)
    assert bucket == datetime(2024, 1, 1, 12, 3, 0, tzinfo=UTC)


def test_candle_close_time() -> None:
    open_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
    close_time = candle_close_time(open_time, Timeframe.M5)
    assert close_time == datetime(2024, 1, 1, 12, 5, 0, tzinfo=UTC)
