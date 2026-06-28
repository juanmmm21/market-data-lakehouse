from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from market_data_lakehouse.aggregator import CandleAggregator, MultiTimeframeAggregator
from market_data_lakehouse.models import ExchangeId, Timeframe, TradeSide, TradeTick


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


def test_candle_aggregator_completes_on_bucket_rollover() -> None:
    aggregator = CandleAggregator(ExchangeId.BINANCE, "BTCUSDT", Timeframe.M1)

    assert aggregator.ingest(_tick("100")) is None
    assert aggregator.ingest(_tick("105", offset_seconds=20)) is None

    completed = aggregator.ingest(_tick("110", offset_seconds=70))

    assert completed is not None
    assert completed.open == Decimal("100")
    assert completed.high == Decimal("105")
    assert completed.close == Decimal("105")
    assert completed.volume == Decimal("2")


def test_multi_timeframe_emits_multiple_completed_candles() -> None:
    engine = MultiTimeframeAggregator(
        ExchangeId.BINANCE,
        "BTCUSDT",
        (Timeframe.M1, Timeframe.M5),
    )

    completed_first = engine.ingest(_tick("100"))
    assert completed_first == ()

    completed_second = engine.ingest(_tick("110", offset_seconds=400))

    assert len(completed_second) == 2
    timeframes = {candle.timeframe for candle in completed_second}
    assert timeframes == {Timeframe.M1, Timeframe.M5}
