from __future__ import annotations

import json
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import pyarrow.parquet as pq

from market_data_lakehouse.depth import DepthLakeWriter, export_depth


def _records() -> list[dict[str, Any]]:
    return [
        {"type": "depth_diff", "symbol": "BTCUSDT", "first_update_id": 1, "final_update_id": 3,
         "event_time": "2026-11-17T23:00:00.100+00:00",
         "received_at": "2026-11-17T23:00:00.150+00:00",
         "bids": [["100.10", "1.5"]], "asks": [["100.20", "0"]]},
        {"type": "trade", "symbol": "BTCUSDT", "trade_id": "9", "price": "100.15",
         "quantity": "0.002", "side": "buy", "event_time": "2026-11-17T23:00:00.120+00:00"},
        {"type": "depth_snapshot", "symbol": "BTCUSDT", "last_update_id": 2,
         "received_at": "2026-11-17T23:00:00.200+00:00",
         "bids": [["100.10", "1"]], "asks": [["100.20", "3"]]},
        {"type": "depth_diff", "symbol": "BTCUSDT", "first_update_id": 4, "final_update_id": 4,
         "event_time": "2026-11-17T23:59:59.900+00:00",
         "received_at": "2026-11-18T00:00:00.010+00:00",
         "bids": [], "asks": [["100.30", "2"]]},
        {"type": "depth_diff", "symbol": "BTCUSDT", "first_update_id": 9, "final_update_id": 5,
         "event_time": "2026-11-17T23:00:01+00:00", "bids": [], "asks": []},
        {"type": "ticker", "symbol": "BTCUSDT"},
    ]


def test_ingest_partitions_by_hour_and_rejects_bad_rows(tmp_path: Path) -> None:
    report = DepthLakeWriter(tmp_path).ingest(_records(), symbol="BTCUSDT")
    assert report.rows == {"snapshots": 1, "diffs": 2, "trades": 1}
    assert report.rejected == 2
    diff_files = sorted((tmp_path / "binance" / "BTCUSDT" / "depth" / "diffs").rglob("*.parquet"))
    assert [f.parent.name for f in diff_files] == ["hour=23"]
    assert diff_files[0].parent.parent.name == "date=2026-11-17"
    table = pq.read_table(diff_files[0])
    assert table.column("bids").to_pylist() == [[["100.10", "1.5"]], []]


def test_export_merges_in_time_order_and_preserves_strings(tmp_path: Path) -> None:
    DepthLakeWriter(tmp_path).ingest(_records(), symbol="BTCUSDT")
    exported = list(export_depth(tmp_path, symbol="btcusdt"))
    assert [r["type"] for r in exported] == ["depth_diff", "trade", "depth_snapshot", "depth_diff"]
    assert exported[0]["bids"] == [["100.10", "1.5"]]
    assert exported[1]["price"] == "100.15"
    assert exported[2]["last_update_id"] == 2
    window = list(
        export_depth(
            tmp_path,
            symbol="BTCUSDT",
            start=datetime(2026, 11, 17, 23, 30, tzinfo=UTC),
        )
    )
    assert [r["final_update_id"] for r in window] == [4]


def test_ingest_is_append_only(tmp_path: Path) -> None:
    writer = DepthLakeWriter(tmp_path)
    writer.ingest(_records()[:1], symbol="BTCUSDT")
    writer.ingest(_records()[:1], symbol="BTCUSDT")
    files = list((tmp_path / "binance" / "BTCUSDT" / "depth" / "diffs").rglob("*.parquet"))
    assert len(files) == 2
    count = duckdb.sql(f"SELECT count(*) FROM read_parquet({[str(f) for f in files]})").fetchone()
    assert count == (2,)


def test_cli_roundtrip(tmp_path: Path) -> None:
    source = tmp_path / "recording.jsonl"
    source.write_text("\n".join(json.dumps(r) for r in _records()[:4]), encoding="utf-8")
    root = tmp_path / "lake"
    out = tmp_path / "export.jsonl"
    cli = [sys.executable, "-m", "market_data_lakehouse"]
    ingest = subprocess.run(
        [*cli, "ingest-depth", "--input", str(source), "--root", str(root), "--symbol", "BTCUSDT"],
        check=True, capture_output=True, text=True,
    )
    assert json.loads(ingest.stdout)["rows"]["diffs"] == 2
    subprocess.run(
        [*cli, "export-depth", "--root", str(root), "--symbol", "BTCUSDT", "--output", str(out)],
        check=True,
    )
    assert len(out.read_text(encoding="utf-8").splitlines()) == 4
