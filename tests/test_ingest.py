from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

import pytest

from market_data_lakehouse.ingest.jsonl_source import JsonlTickSource, parse_trade_tick
from market_data_lakehouse.models import ExchangeId, TradeSide


def test_parse_trade_tick_from_dict() -> None:
    tick = parse_trade_tick(
        {
            "exchange": "binance",
            "symbol": "BTCUSDT",
            "trade_id": "1",
            "price": "100.5",
            "quantity": "0.01",
            "side": "buy",
            "event_time": "2024-01-01T12:00:00Z",
        },
    )

    assert tick.exchange == ExchangeId.BINANCE
    assert tick.price == Decimal("100.5")
    assert tick.side == TradeSide.BUY


@pytest.mark.asyncio
async def test_jsonl_tick_source_reads_file(tmp_path: Path) -> None:
    file_path = tmp_path / "ticks.jsonl"
    file_path.write_text(
        json.dumps(
            {
                "exchange": "binance",
                "symbol": "BTCUSDT",
                "trade_id": "1",
                "price": "100",
                "quantity": "1",
                "side": "buy",
                "event_time": "2024-01-01T12:00:00Z",
            },
        )
        + "\n",
        encoding="utf-8",
    )

    source = JsonlTickSource(file_path)
    ticks = [tick async for tick in source.ticks()]

    assert len(ticks) == 1
    assert ticks[0].symbol == "BTCUSDT"
