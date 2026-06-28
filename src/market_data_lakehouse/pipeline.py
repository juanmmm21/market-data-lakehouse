from __future__ import annotations

import logging

from market_data_lakehouse.aggregator import MultiTimeframeAggregator
from market_data_lakehouse.models import Candle, LakehouseConfig
from market_data_lakehouse.protocols import TickSource
from market_data_lakehouse.storage.duckdb_catalog import DuckDBLakeCatalog
from market_data_lakehouse.writers.parquet_writer import ParquetLakeWriter

logger = logging.getLogger(__name__)


class MarketDataLakehousePipeline:
    """Pipeline que agrega ticks en velas y las persiste en Parquet + DuckDB."""

    def __init__(self, config: LakehouseConfig) -> None:
        self._config = config
        self._aggregator = MultiTimeframeAggregator(
            exchange=config.exchange,
            symbol=config.symbol,
            timeframes=config.timeframes,
        )
        self._writer = ParquetLakeWriter(config.root_path)
        duckdb_path = config.duckdb_path or f"{config.root_path.rstrip('/')}/catalog.duckdb"
        self._catalog = DuckDBLakeCatalog(duckdb_path=duckdb_path, parquet_writer=self._writer)
        self._pending: list[Candle] = []

    @property
    def catalog(self) -> DuckDBLakeCatalog:
        return self._catalog

    async def run(self, source: TickSource) -> int:
        processed = 0
        try:
            async for tick in _async_iter(source):
                processed += 1
                completed = self._aggregator.ingest(tick)
                if completed:
                    self._pending.extend(completed)
                if len(self._pending) >= self._config.flush_batch_size:
                    await self._flush_pending()
        except Exception:
            logger.exception(
                "pipeline failed exchange=%s symbol=%s",
                self._config.exchange.value,
                self._config.symbol,
            )
            raise
        finally:
            await self.flush()

        return processed

    async def flush(self) -> None:
        completed = self._aggregator.flush()
        if completed:
            self._pending.extend(completed)
        await self._flush_pending()
        self._catalog.refresh()

    async def _flush_pending(self) -> None:
        if not self._pending:
            return
        batch = tuple(self._pending)
        self._pending.clear()
        await self._writer.write_candles(batch)
        logger.info("flushed candles count=%s symbol=%s", len(batch), self._config.symbol)


async def _async_iter(source: TickSource):
    async for tick in source.ticks():
        yield tick
