from __future__ import annotations

import json
import logging
from collections.abc import AsyncIterator
from datetime import datetime
from decimal import Decimal
from pathlib import Path

from market_data_lakehouse.models import (
    ExchangeId,
    TradeSide,
    TradeTick,
    utc_from_iso8601,
    utc_from_millis,
)

logger = logging.getLogger(__name__)


class JsonlTickSource:
    """Fuente de ticks desde un archivo JSON Lines para replays y pruebas."""

    def __init__(self, file_path: str | Path) -> None:
        self._file_path = Path(file_path)
        if not self._file_path.exists():
            raise FileNotFoundError(f"tick file not found: {self._file_path}")

    async def ticks(self) -> AsyncIterator[TradeTick]:
        try:
            with self._file_path.open("r", encoding="utf-8") as handle:
                for line_number, line in enumerate(handle, start=1):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        payload = json.loads(line)
                    except json.JSONDecodeError as exc:
                        logger.exception("invalid json at line=%s", line_number)
                        raise ValueError(f"invalid json at line {line_number}") from exc

                    if not isinstance(payload, dict):
                        raise ValueError(f"line {line_number} must contain a JSON object")

                    yield parse_trade_tick(payload)
        except OSError as exc:
            logger.exception("failed to read tick file path=%s", self._file_path)
            raise RuntimeError(f"failed to read tick file: {self._file_path}") from exc


def parse_trade_tick(payload: dict[str, object]) -> TradeTick:
    """Normaliza un dict compatible con `TradeEvent` del ecosistema quant-core."""
    exchange_raw = payload.get("exchange")
    symbol = payload.get("symbol")
    trade_id = payload.get("trade_id")
    price = payload.get("price")
    quantity = payload.get("quantity")
    side_raw = payload.get("side")
    event_time_raw = payload.get("event_time")

    if not isinstance(exchange_raw, str):
        raise ValueError("exchange must be a string")
    if not isinstance(symbol, str):
        raise ValueError("symbol must be a string")
    if not isinstance(trade_id, str):
        raise ValueError("trade_id must be a string")
    if not isinstance(price, str):
        raise ValueError("price must be a string")
    if not isinstance(quantity, str):
        raise ValueError("quantity must be a string")
    if not isinstance(side_raw, str):
        raise ValueError("side must be a string")

    event_time = _parse_event_time(event_time_raw)

    return TradeTick(
        exchange=ExchangeId(exchange_raw),
        symbol=symbol,
        trade_id=trade_id,
        price=Decimal(price),
        quantity=Decimal(quantity),
        side=TradeSide(side_raw),
        event_time=event_time,
    )


def _parse_event_time(value: object) -> datetime:
    if isinstance(value, str):
        return utc_from_iso8601(value)
    if isinstance(value, int):
        return utc_from_millis(value)
    if isinstance(value, float):
        return utc_from_millis(int(value))
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise ValueError("event_time must be timezone-aware")
        return value
    raise ValueError("event_time must be iso string, epoch millis or datetime")
