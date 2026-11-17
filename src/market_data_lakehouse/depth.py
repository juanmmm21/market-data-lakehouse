"""L2 order-book storage: REST snapshots, diff-depth updates and raw trades.

Layout (Hive-style, append-only: each ingest writes new part files, nothing is rewritten)::

    <root>/<exchange>/<symbol>/depth/<dataset>/date=YYYY-MM-DD/hour=HH/part-<uuid>.parquet

with ``dataset`` one of ``snapshots``, ``diffs`` and ``trades``. Prices and quantities stay
strings, as in the candle tables, so no precision is lost. Levels are ``list<list<string>>``
(``[[price, quantity], ...]``) exactly as Binance sends them.

``export_depth`` merges the three datasets back into one time-ordered JSONL stream, the input of
``order-book-reconstructor replay``.
"""

from __future__ import annotations

import json
import logging
import uuid
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

LEVELS = pa.list_(pa.list_(pa.string()))
TS = pa.timestamp("us", tz="UTC")

SCHEMAS: dict[str, pa.Schema] = {
    "snapshots": pa.schema(
        [
            ("exchange", pa.string()),
            ("symbol", pa.string()),
            ("last_update_id", pa.int64()),
            ("received_at", TS),
            ("bids", LEVELS),
            ("asks", LEVELS),
        ]
    ),
    "diffs": pa.schema(
        [
            ("exchange", pa.string()),
            ("symbol", pa.string()),
            ("first_update_id", pa.int64()),
            ("final_update_id", pa.int64()),
            ("event_time", TS),
            ("received_at", TS),
            ("bids", LEVELS),
            ("asks", LEVELS),
        ]
    ),
    "trades": pa.schema(
        [
            ("exchange", pa.string()),
            ("symbol", pa.string()),
            ("trade_id", pa.string()),
            ("price", pa.string()),
            ("quantity", pa.string()),
            ("side", pa.string()),
            ("event_time", TS),
        ]
    ),
}

RECORD_TYPES = {"depth_snapshot": "snapshots", "depth_diff": "diffs", "trade": "trades"}


def _ts(value: Any) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _levels(raw: Any, name: str) -> list[list[str]]:
    if not isinstance(raw, list):
        raise ValueError(f"{name} must be a list")
    result: list[list[str]] = []
    for entry in raw:
        if not isinstance(entry, list) or len(entry) != 2:
            raise ValueError(f"malformed level in {name}")
        result.append([str(entry[0]), str(entry[1])])
    return result


def to_row(record: dict[str, Any], *, exchange: str, symbol: str) -> tuple[str, dict[str, Any]]:
    """Validate one recorded JSON object and map it to (dataset, Parquet row)."""
    kind = record.get("type")
    dataset = RECORD_TYPES.get(str(kind))
    if dataset is None:
        raise ValueError(f"unsupported record type: {kind!r}")
    base = {
        "exchange": str(record.get("exchange", exchange)),
        "symbol": str(record.get("symbol", symbol)).upper(),
    }
    if dataset == "snapshots":
        return dataset, {
            **base,
            "last_update_id": int(record["last_update_id"]),
            "received_at": _ts(record["received_at"]),
            "bids": _levels(record["bids"], "bids"),
            "asks": _levels(record["asks"], "asks"),
        }
    if dataset == "diffs":
        first, final = int(record["first_update_id"]), int(record["final_update_id"])
        if final < first:
            raise ValueError("final_update_id must be >= first_update_id")
        event_time = _ts(record["event_time"])
        return dataset, {
            **base,
            "first_update_id": first,
            "final_update_id": final,
            "event_time": event_time,
            "received_at": _ts(record.get("received_at") or event_time.isoformat()),
            "bids": _levels(record["bids"], "bids"),
            "asks": _levels(record["asks"], "asks"),
        }
    return dataset, {
        **base,
        "trade_id": str(record["trade_id"]),
        "price": str(record["price"]),
        "quantity": str(record["quantity"]),
        "side": str(record["side"]),
        "event_time": _ts(record["event_time"]),
    }


@dataclass(slots=True)
class DepthIngestReport:
    rows: dict[str, int] = field(default_factory=lambda: dict.fromkeys(SCHEMAS, 0))
    files: int = 0
    rejected: int = 0


class DepthLakeWriter:
    def __init__(self, root: str | Path) -> None:
        self._root = Path(root)

    def dataset_dir(self, exchange: str, symbol: str, dataset: str) -> Path:
        return self._root / exchange / symbol.upper() / "depth" / dataset

    def ingest(
        self,
        records: Iterable[dict[str, Any]],
        *,
        exchange: str = "binance",
        symbol: str,
        batch_size: int = 20_000,
    ) -> DepthIngestReport:
        report = DepthIngestReport()
        buckets: dict[tuple[str, str, str, str], list[dict[str, Any]]] = {}
        pending = 0
        for record in records:
            try:
                dataset, row = to_row(record, exchange=exchange, symbol=symbol)
            except (KeyError, ValueError, TypeError) as exc:
                report.rejected += 1
                logger.warning("rejected depth record: %s", exc)
                continue
            moment: datetime = row.get("event_time") or row["received_at"]
            key = (dataset, row["exchange"], row["symbol"], moment.strftime("%Y-%m-%d/%H"))
            buckets.setdefault(key, []).append(row)
            report.rows[dataset] += 1
            pending += 1
            if pending >= batch_size:
                report.files += self._flush(buckets)
                pending = 0
        report.files += self._flush(buckets)
        return report

    def _flush(self, buckets: dict[tuple[str, str, str, str], list[dict[str, Any]]]) -> int:
        written = 0
        for (dataset, exchange, symbol, day_hour), rows in buckets.items():
            if not rows:
                continue
            day, hour = day_hour.split("/")
            directory = self.dataset_dir(exchange, symbol, dataset) / f"date={day}" / f"hour={hour}"
            directory.mkdir(parents=True, exist_ok=True)
            table = pa.Table.from_pylist(rows, schema=SCHEMAS[dataset])
            pq.write_table(
                table, directory / f"part-{uuid.uuid4().hex}.parquet", compression="zstd"
            )
            written += 1
        buckets.clear()
        return written


def _files(root: Path, exchange: str, symbol: str, dataset: str) -> list[str]:
    base = DepthLakeWriter(root).dataset_dir(exchange, symbol, dataset)
    return sorted(str(path) for path in base.rglob("*.parquet")) if base.exists() else []


def export_depth(
    root: str | Path,
    *,
    symbol: str,
    exchange: str = "binance",
    start: datetime | None = None,
    end: datetime | None = None,
) -> Iterator[dict[str, Any]]:
    """Merged, time-ordered JSONL records (snapshots by receipt, diffs/trades by event time)."""
    root_path = Path(root)
    lo = start or datetime(1970, 1, 1, tzinfo=UTC)
    hi = end or datetime(9999, 1, 1, tzinfo=UTC)
    selects: list[str] = []
    params: list[Any] = []
    sources = {
        "snapshots": (
            "SELECT received_at AS t, 0 AS o, 'depth_snapshot' AS type, last_update_id, "
            "NULL::BIGINT AS first_update_id, NULL::BIGINT AS final_update_id, received_at, "
            "NULL::TIMESTAMPTZ AS event_time, bids, asks, NULL AS trade_id, NULL AS price, "
            "NULL AS quantity, NULL AS side FROM read_parquet(?) "
            "WHERE received_at BETWEEN ? AND ?"
        ),
        "diffs": (
            "SELECT event_time AS t, 1 AS o, 'depth_diff', NULL::BIGINT, first_update_id, "
            "final_update_id, received_at, event_time, bids, asks, NULL, NULL, NULL, NULL "
            "FROM read_parquet(?) WHERE event_time BETWEEN ? AND ?"
        ),
        "trades": (
            "SELECT event_time AS t, 2 AS o, 'trade', NULL::BIGINT, NULL::BIGINT, NULL::BIGINT, "
            "NULL::TIMESTAMPTZ, event_time, NULL, NULL, trade_id, price, quantity, side "
            "FROM read_parquet(?) WHERE event_time BETWEEN ? AND ?"
        ),
    }
    for dataset, sql in sources.items():
        files = _files(root_path, exchange, symbol, dataset)
        if files:
            selects.append(sql)
            params += [files, lo, hi]
    if not selects:
        return
    query = " UNION ALL ".join(selects) + " ORDER BY t, o, final_update_id"
    connection = duckdb.connect()
    try:
        cursor = connection.execute(query, params)
        while True:
            chunk = cursor.fetchmany(5000)
            if not chunk:
                break
            for row in chunk:
                yield _record_from_row(row, exchange=exchange, symbol=symbol.upper())
    finally:
        connection.close()


def _record_from_row(row: tuple[Any, ...], *, exchange: str, symbol: str) -> dict[str, Any]:
    (
        _t,
        _o,
        kind,
        last_id,
        first_id,
        final_id,
        received,
        event,
        bids,
        asks,
        trade_id,
        price,
        quantity,
        side,
    ) = row
    base = {"type": kind, "exchange": exchange, "symbol": symbol}
    if kind == "depth_snapshot":
        return {
            **base,
            "last_update_id": last_id,
            "received_at": received.isoformat(),
            "bids": bids,
            "asks": asks,
        }
    if kind == "depth_diff":
        return {
            **base,
            "first_update_id": first_id,
            "final_update_id": final_id,
            "event_time": event.isoformat(),
            "received_at": received.isoformat(),
            "bids": bids,
            "asks": asks,
        }
    return {
        **base,
        "trade_id": trade_id,
        "price": price,
        "quantity": quantity,
        "side": side,
        "event_time": event.isoformat(),
    }


def read_records(paths: Iterable[str | Path]) -> Iterator[dict[str, Any]]:
    for path in paths:
        with Path(path).open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if line:
                    record = json.loads(line)
                    if isinstance(record, dict):
                        yield record
