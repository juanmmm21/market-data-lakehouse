from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Protocol

from market_data_lakehouse.models import Candle, LakehouseConfig, TradeTick


class TickSource(Protocol):
    def ticks(self) -> AsyncIterator[TradeTick]:
        """An async generator of ticks (``async def ticks(self): yield ...``)."""
        ...


class CandleSink(Protocol):
    async def write_candles(self, candles: tuple[Candle, ...]) -> None:
        ...


class CandleQueryEngine(Protocol):
    def query_candles(
        self,
        symbol: str,
        timeframe: str,
        *,
        limit: int = 100,
    ) -> list[dict[str, object]]:
        ...


TickHandler = Callable[[TradeTick], Awaitable[None]]
CandleHandler = Callable[[Candle], Awaitable[None]]


class LakehousePipeline(Protocol):
    def __init__(self, config: LakehouseConfig) -> None:
        ...

    async def run(self, source: TickSource) -> int:
        ...

    async def flush(self) -> None:
        ...
