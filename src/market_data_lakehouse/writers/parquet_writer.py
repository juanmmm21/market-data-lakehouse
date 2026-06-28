from __future__ import annotations

import logging
from datetime import UTC
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from market_data_lakehouse.models import Candle

logger = logging.getLogger(__name__)

CANDLE_SCHEMA = pa.schema(
    [
        ("exchange", pa.string()),
        ("symbol", pa.string()),
        ("timeframe", pa.string()),
        ("open_time", pa.timestamp("us", tz="UTC")),
        ("close_time", pa.timestamp("us", tz="UTC")),
        ("open", pa.string()),
        ("high", pa.string()),
        ("low", pa.string()),
        ("close", pa.string()),
        ("volume", pa.string()),
        ("trade_count", pa.int64()),
    ],
)


class ParquetLakeWriter:
    """Escribe velas en archivos Parquet particionados por exchange/symbol/timeframe/fecha."""

    def __init__(self, root_path: str | Path) -> None:
        self._root_path = Path(root_path)
        self._root_path.mkdir(parents=True, exist_ok=True)

    @property
    def root_path(self) -> Path:
        return self._root_path

    async def write_candles(self, candles: tuple[Candle, ...]) -> None:
        if not candles:
            return

        grouped: dict[Path, list[Candle]] = {}
        for candle in candles:
            path = self._partition_path(candle)
            grouped.setdefault(path, []).append(candle)

        for path, batch in grouped.items():
            try:
                self._append_batch(path, batch)
            except Exception:
                logger.exception("failed to write parquet batch path=%s", path)
                raise

    def _partition_path(self, candle: Candle) -> Path:
        open_time = candle.open_time.astimezone(UTC)
        return (
            self._root_path
            / candle.exchange.value
            / candle.symbol
            / candle.timeframe.value
            / f"year={open_time.year}"
            / f"month={open_time.month:02d}"
            / f"day={open_time.day:02d}"
            / "candles.parquet"
        )

    def _append_batch(self, path: Path, candles: list[Candle]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        rows = [_candle_to_row(candle) for candle in candles]
        table = pa.Table.from_pylist(rows, schema=CANDLE_SCHEMA)

        if path.exists():
            existing = pq.read_table(path, schema=CANDLE_SCHEMA)
            table = pa.concat_tables([existing, table])

        pq.write_table(table, path, compression="zstd")

    def glob_pattern(self) -> str:
        return str(self._root_path / "**" / "candles.parquet")


def _candle_to_row(candle: Candle) -> dict[str, object]:
    return {
        "exchange": candle.exchange.value,
        "symbol": candle.symbol,
        "timeframe": candle.timeframe.value,
        "open_time": candle.open_time,
        "close_time": candle.close_time,
        "open": str(candle.open),
        "high": str(candle.high),
        "low": str(candle.low),
        "close": str(candle.close),
        "volume": str(candle.volume),
        "trade_count": candle.trade_count,
    }
