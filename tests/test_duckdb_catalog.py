from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from market_data_lakehouse.models import Candle, ExchangeId, Timeframe
from market_data_lakehouse.storage.duckdb_catalog import DuckDBLakeCatalog
from market_data_lakehouse.writers.parquet_writer import ParquetLakeWriter


@pytest.mark.asyncio
async def test_duckdb_catalog_queries_written_candles(tmp_path: Path) -> None:
    writer = ParquetLakeWriter(tmp_path)
    candle = Candle(
        exchange=ExchangeId.BINANCE,
        symbol="BTCUSDT",
        timeframe=Timeframe.M1,
        open_time=datetime(2024, 1, 2, 15, 30, tzinfo=UTC),
        close_time=datetime(2024, 1, 2, 15, 31, tzinfo=UTC),
        open=Decimal("100"),
        high=Decimal("110"),
        low=Decimal("90"),
        close=Decimal("105"),
        volume=Decimal("3"),
        trade_count=3,
    )
    await writer.write_candles((candle,))

    catalog = DuckDBLakeCatalog(tmp_path / "catalog.duckdb", writer)
    try:
        rows = catalog.query_candles("BTCUSDT", "1m", limit=10)
    finally:
        catalog.close()

    assert len(rows) == 1
    assert rows[0]["symbol"] == "BTCUSDT"
    assert rows[0]["close"] == "105"
