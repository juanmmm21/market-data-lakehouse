# market-data-lakehouse

Local **data lake** pipeline that aggregates real-time ticks into OHLCV candles (`1m`, `5m`, `1h`), persists them in **Parquet** partitions, and exposes a **DuckDB** catalog for analytical queries. Third module of the [quant-core-infra](https://github.com/juanmmm21/quant-core-infra) ecosystem.

Repository: [github.com/juanmmm21/market-data-lakehouse](https://github.com/juanmmm21/market-data-lakehouse)

---

## What it is and what problem it solves

Individual ticks are too granular for technical analysis, candle-based backtesting, or efficient long-term storage. This module transforms millions of trades into **OHLCV candles** with fixed time windows and stores them in a compressed columnar format (Parquet) queryable via SQL (DuckDB).

It solves three needs at once:

1. **Streaming aggregation** — build candles on the fly without waiting for the bucket to close
2. **Analytical persistence** — Parquet partitioned by exchange, symbol, timeframe, and date
3. **Offline replay** — ingest tick JSONL to reproduce historical data without a WebSocket

---

## Role in quant-core-infra

```text
websocket-feed-handler ──► ticks ──► market-data-lakehouse ──► OHLCV candles
                                           │
                                           ├──► ta-indicators-from-scratch
                                           └──► alpha-signal-generator (via JSONL)
```

It is the ecosystem's **local time-series store**. Technical indicators and strategies consume its candles, not the raw ticks.

---

## Objective

Demonstrates:

- Multi-timeframe OHLCV aggregation from normalized ticks
- Partitioned storage optimized for analytics
- DuckDB catalog over Parquet files
- JSONL ingestion compatible with `websocket-feed-handler`

---

## How it works

1. **Ingestion:** `JsonlTickSource` reads ticks line by line (or a future async stream).
2. **Aggregation:** `MultiTimeframeAggregator` maintains an active bucket per timeframe (`1m`, `5m`, `1h`).
3. **Per tick:** updates `open/high/low/close/volume/trade_count` of the current bucket.
4. **Roll forward:** when a tick falls into a new bucket, the closed candle is emitted to the writer.
5. **Write:** `ParquetLakeWriter` accumulates candles in batches and writes them in Hive-style partitions.
6. **Catalog:** `DuckDBLakeCatalog` registers the Parquet paths and enables direct SQL.

### Partition structure

```text
data/lake/
└── binance/
    └── BTCUSDT/
        ├── 1m/year=2024/month=01/day=01/part-000.parquet
        ├── 5m/year=2024/month=01/day=01/part-000.parquet
        └── 1h/year=2024/month=01/day=01/part-000.parquet
```

---

## Architecture

```text
Trade ticks (JSONL / stream)
        │
        ▼
MultiTimeframeAggregator (1m, 5m, 1h)
        │
        ▼
ParquetLakeWriter ──► ZSTD partitions
        │
        ▼
DuckDBLakeCatalog ──► SELECT ... FROM candles
```

### Components

| Module | Responsibility |
|--------|----------------|
| `aggregator.py` | Builds candles and rolls buckets |
| `timeframes.py` | UTC bucket alignment |
| `writers/parquet_writer.py` | Compressed Parquet writing |
| `storage/duckdb_catalog.py` | SQL view over all files |
| `ingest/jsonl_source.py` | Replay from JSON Lines |
| `pipeline.py` | Ingest + flush orchestration |

### Technical decisions

- **Decimal** during aggregation; strings in Parquet to preserve precision
- **Hive partitions** by exchange, symbol, timeframe, and date
- **Configurable batch flush** to reduce I/O under high tick rate
- **Timezone-aware** — all buckets in UTC

---

## Data model

### Input tick (`TradeTick`)

| Field | Type | Notes |
|-------|------|-------|
| `exchange` | `binance` / `coinbase` | |
| `symbol` | `str` | |
| `price`, `quantity` | `Decimal` | |
| `side` | `buy` / `sell` | |
| `event_time` | `datetime` UTC | |

### Output candle (`Candle`)

| Field | Description |
|-------|-------------|
| `open`, `high`, `low`, `close` | OHLC as Decimal |
| `volume` | Sum of quantities |
| `trade_count` | Number of ticks in the candle |
| `open_time`, `close_time` | Bucket bounds |

---

## L2 depth storage (snapshots, diffs, trades)

Candles are not enough to simulate limit orders: a passive order's fate depends on how much
volume sits ahead of it at its price. The lakehouse therefore also stores raw L2 data recorded by
`websocket-feed-handler --channels trade depth` plus REST snapshots:

```text
data/lake/<exchange>/<symbol>/depth/
├── snapshots/date=2026-11-17/hour=23/part-<uuid>.parquet   last_update_id, received_at, bids, asks
├── diffs/date=2026-11-17/hour=23/part-<uuid>.parquet       first/final_update_id, event_time, received_at, bids, asks
└── trades/date=2026-11-17/hour=23/part-<uuid>.parquet      trade_id, price, quantity, side, event_time
```

- **Append-only**: each ingest writes new part files; nothing is read back and rewritten.
- Levels stay `[[price, quantity], ...]` **strings**, exactly as Binance sends them.
- `export-depth` merges the three datasets into one time-ordered JSONL (snapshots by receipt time,
  diffs and trades by event time), the input of `order-book-reconstructor replay`, which checks
  update-id continuity and resynchronises on gaps.

```bash
market-data-lakehouse ingest-depth --input stream.jsonl snapshots.jsonl --root data/lake --symbol BTCUSDT
market-data-lakehouse export-depth --root data/lake --symbol BTCUSDT \
  --start 2026-11-17T23:20:00Z --end 2026-11-18T00:00:00Z --output depth.jsonl
```

Measured on 2026-11-17 (BTCUSDT, Binance `data-stream.binance.vision`, 6.5 minutes: 3,856 diffs,
10,070 trades, 2 snapshots of 5,000 levels): 7.2 MB of JSONL became 832 KB of Parquet (zstd);
ingest 0.46 s and export 0.59 s wall time on a 4-vCPU Linux x86_64 container. Exported and
replayed, the book matched the REST snapshot on all 400 compared levels.

---

## Requirements

- Python **3.11+**

---

## Installation

```bash
cd market-data-lakehouse
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

---

## CLI usage

### Ingest ticks

```bash
market-data-lakehouse ingest \
  --input samples/btcusdt_ticks.jsonl \
  --exchange binance \
  --symbol BTCUSDT \
  --root data/lake \
  --timeframes 1m 5m 1h \
  --flush-batch-size 50
```

### Query candles

```bash
market-data-lakehouse query \
  --root data/lake \
  --symbol BTCUSDT \
  --timeframe 1m \
  --limit 10
```

---

## Tick JSONL format

```json
{
  "exchange": "binance",
  "symbol": "BTCUSDT",
  "trade_id": "12345",
  "price": "67234.50",
  "quantity": "0.015",
  "side": "buy",
  "event_time": "2024-01-01T12:00:01Z"
}
```

---

## Programmatic usage

```python
import asyncio

from market_data_lakehouse import (
    ExchangeId,
    JsonlTickSource,
    LakehouseConfig,
    MarketDataLakehousePipeline,
    Timeframe,
)


async def main() -> None:
    config = LakehouseConfig(
        root_path="data/lake",
        exchange=ExchangeId.BINANCE,
        symbol="BTCUSDT",
        timeframes=(Timeframe.M1, Timeframe.M5, Timeframe.H1),
        flush_batch_size=100,
    )
    pipeline = MarketDataLakehousePipeline(config)
    processed = await pipeline.run(JsonlTickSource("ticks.jsonl"))
    print(f"processed {processed} ticks")


if __name__ == "__main__":
    asyncio.run(main())
```

---

## Development

```bash
pytest -q
ruff check src tests
mypy src
```

---

## Roadmap

- [ ] Direct ingestion from `websocket-feed-handler` without intermediate JSONL
- [ ] Support for additional timeframes (`15m`, `4h`, `1d`)
- [ ] Automatic compaction of small partitions

---

## License

MIT
