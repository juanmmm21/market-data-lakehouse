from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum


class ExchangeId(StrEnum):
    BINANCE = "binance"
    COINBASE = "coinbase"


class Timeframe(StrEnum):
    M1 = "1m"
    M5 = "5m"
    H1 = "1h"

    @property
    def seconds(self) -> int:
        mapping = {
            Timeframe.M1: 60,
            Timeframe.M5: 300,
            Timeframe.H1: 3600,
        }
        return mapping[self]


class TradeSide(StrEnum):
    BUY = "buy"
    SELL = "sell"


@dataclass(frozen=True, slots=True)
class TradeTick:
    """Tick normalizado usado como entrada del agregador de velas."""

    exchange: ExchangeId
    symbol: str
    trade_id: str
    price: Decimal
    quantity: Decimal
    side: TradeSide
    event_time: datetime

    def __post_init__(self) -> None:
        if self.price <= Decimal("0"):
            raise ValueError("price must be positive")
        if self.quantity <= Decimal("0"):
            raise ValueError("quantity must be positive")
        if self.event_time.tzinfo is None:
            raise ValueError("event_time must be timezone-aware")


@dataclass(frozen=True, slots=True)
class Candle:
    exchange: ExchangeId
    symbol: str
    timeframe: Timeframe
    open_time: datetime
    close_time: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    trade_count: int

    def __post_init__(self) -> None:
        if self.trade_count <= 0:
            raise ValueError("trade_count must be positive")
        if self.open_time >= self.close_time:
            raise ValueError("open_time must be before close_time")


@dataclass(frozen=True, slots=True)
class LakehouseConfig:
    root_path: str
    exchange: ExchangeId
    symbol: str
    timeframes: tuple[Timeframe, ...] = (Timeframe.M1, Timeframe.M5, Timeframe.H1)
    duckdb_path: str | None = None
    flush_batch_size: int = 100

    def __post_init__(self) -> None:
        if not self.root_path:
            raise ValueError("root_path must not be empty")
        if not self.symbol:
            raise ValueError("symbol must not be empty")
        if not self.timeframes:
            raise ValueError("timeframes must not be empty")
        if self.flush_batch_size <= 0:
            raise ValueError("flush_batch_size must be positive")


def utc_from_millis(timestamp_ms: int) -> datetime:
    return datetime.fromtimestamp(timestamp_ms / 1000.0, tz=UTC)


def utc_from_iso8601(value: str) -> datetime:
    normalized = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)
