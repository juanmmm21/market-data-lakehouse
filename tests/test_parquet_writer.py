from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from market_data_lakehouse.models import Candle, ExchangeId, Timeframe
from market_data_lakehouse.writers.parquet_writer import ParquetLakeWriter


def _candle(open_time: datetime, close: str) -> Candle:
    return Candle(
        exchange=ExchangeId.BINANCE,
        symbol="BTCUSDT",
        timeframe=Timeframe.M1,
        open_time=open_time,
        close_time=open_time + timedelta(minutes=1),
        open=Decimal("100"),
        high=Decimal("110"),
        low=Decimal("90"),
        close=Decimal(close),
        volume=Decimal("3"),
        trade_count=3,
    )


@pytest.mark.asyncio
async def test_parquet_writer_creates_partitioned_file(tmp_path: Path) -> None:
    writer = ParquetLakeWriter(tmp_path)
    open_time = datetime(2024, 1, 2, 15, 30, tzinfo=UTC)
    await writer.write_candles((_candle(open_time, "105"),))

    parquet_files = list(tmp_path.rglob("candles.parquet"))
    assert len(parquet_files) == 1

    table = pq.read_table(parquet_files[0])
    assert table.num_rows == 1
    assert table.column("close")[0].as_py() == "105"
