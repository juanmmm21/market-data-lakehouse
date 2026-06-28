from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from market_data_lakehouse.ingest.jsonl_source import JsonlTickSource
from market_data_lakehouse.models import ExchangeId, LakehouseConfig, Timeframe
from market_data_lakehouse.pipeline import MarketDataLakehousePipeline


@pytest.mark.asyncio
async def test_pipeline_ingests_jsonl_and_materializes_candles(tmp_path: Path) -> None:
    ticks_path = tmp_path / "ticks.jsonl"
    base = datetime(2024, 1, 1, 12, 0, 5, tzinfo=UTC)
    lines = []
    for index in range(3):
        event_time = (base + timedelta(seconds=index * 30)).isoformat().replace("+00:00", "Z")
        lines.append(
            json.dumps(
                {
                    "exchange": "binance",
                    "symbol": "BTCUSDT",
                    "trade_id": str(index),
                    "price": str(100 + index),
                    "quantity": "1",
                    "side": "buy",
                    "event_time": event_time,
                },
            ),
        )
    ticks_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    config = LakehouseConfig(
        root_path=str(tmp_path / "lake"),
        exchange=ExchangeId.BINANCE,
        symbol="BTCUSDT",
        timeframes=(Timeframe.M1,),
        duckdb_path=str(tmp_path / "catalog.duckdb"),
        flush_batch_size=1,
    )
    pipeline = MarketDataLakehousePipeline(config)
    processed = await pipeline.run(JsonlTickSource(ticks_path))

    assert processed == 3
    parquet_files = list((tmp_path / "lake").rglob("candles.parquet"))
    assert parquet_files

    rows = pipeline.catalog.query_candles("BTCUSDT", "1m", limit=5)
    assert rows
