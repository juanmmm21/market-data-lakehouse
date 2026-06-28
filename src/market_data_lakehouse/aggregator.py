from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from market_data_lakehouse.models import Candle, ExchangeId, Timeframe, TradeTick
from market_data_lakehouse.timeframes import candle_close_time, floor_to_timeframe


@dataclass(slots=True)
class _MutableCandle:
    exchange: ExchangeId
    symbol: str
    timeframe: Timeframe
    open_time: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    trade_count: int

    def to_candle(self) -> Candle:
        return Candle(
            exchange=self.exchange,
            symbol=self.symbol,
            timeframe=self.timeframe,
            open_time=self.open_time,
            close_time=candle_close_time(self.open_time, self.timeframe),
            open=self.open,
            high=self.high,
            low=self.low,
            close=self.close,
            volume=self.volume,
            trade_count=self.trade_count,
        )


class CandleAggregator:
    """Agrega ticks en una única vela OHLCV para un timeframe."""

    def __init__(self, exchange: ExchangeId, symbol: str, timeframe: Timeframe) -> None:
        self._exchange = exchange
        self._symbol = symbol
        self._timeframe = timeframe
        self._current: _MutableCandle | None = None

    def ingest(self, tick: TradeTick) -> Candle | None:
        if tick.exchange != self._exchange or tick.symbol != self._symbol:
            raise ValueError("tick exchange or symbol mismatch")

        bucket_open = floor_to_timeframe(tick.event_time, self._timeframe)
        completed: Candle | None = None

        if self._current is None:
            self._current = _MutableCandle(
                exchange=tick.exchange,
                symbol=tick.symbol,
                timeframe=self._timeframe,
                open_time=bucket_open,
                open=tick.price,
                high=tick.price,
                low=tick.price,
                close=tick.price,
                volume=tick.quantity,
                trade_count=1,
            )
            return None

        if bucket_open > self._current.open_time:
            completed = self._current.to_candle()
            self._current = _MutableCandle(
                exchange=tick.exchange,
                symbol=tick.symbol,
                timeframe=self._timeframe,
                open_time=bucket_open,
                open=tick.price,
                high=tick.price,
                low=tick.price,
                close=tick.price,
                volume=tick.quantity,
                trade_count=1,
            )
            return completed

        if bucket_open < self._current.open_time:
            raise ValueError("tick arrived in a bucket older than the active candle")

        self._current.high = max(self._current.high, tick.price)
        self._current.low = min(self._current.low, tick.price)
        self._current.close = tick.price
        self._current.volume += tick.quantity
        self._current.trade_count += 1
        return None

    def flush(self) -> Candle | None:
        if self._current is None:
            return None
        completed = self._current.to_candle()
        self._current = None
        return completed


class MultiTimeframeAggregator:
    """Motor que mantiene agregadores paralelos para varios timeframes."""

    def __init__(
        self,
        exchange: ExchangeId,
        symbol: str,
        timeframes: tuple[Timeframe, ...],
    ) -> None:
        self._aggregators = {
            timeframe: CandleAggregator(exchange, symbol, timeframe) for timeframe in timeframes
        }

    def ingest(self, tick: TradeTick) -> tuple[Candle, ...]:
        completed: list[Candle] = []
        for aggregator in self._aggregators.values():
            candle = aggregator.ingest(tick)
            if candle is not None:
                completed.append(candle)
        return tuple(completed)

    def flush(self) -> tuple[Candle, ...]:
        completed: list[Candle] = []
        for aggregator in self._aggregators.values():
            candle = aggregator.flush()
            if candle is not None:
                completed.append(candle)
        return tuple(completed)
