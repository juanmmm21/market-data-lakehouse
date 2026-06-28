# market-data-lakehouse

Local **data lake pipeline** that aggregates live ticks into OHLCV candles (`1m`, `5m`, `1h`) and stores them in **Parquet** partitions with a **DuckDB** catalog for analytics. Third module of the [quant-core-infra](https://github.com/juanmmm21/quant-core-infra) ecosystem.

Repository: [github.com/juanmmm21/market-data-lakehouse](https://github.com/juanmmm21/market-data-lakehouse)

---

## Objective

This project demonstrates:

- Real-time OHLCV aggregation from normalized trade ticks
- Partitioned Parquet storage optimized for analytics
- DuckDB catalog for fast SQL queries over historical candles
- JSONL replay ingestion compatible with `websocket-feed-handler` output

---

## Architecture

```text
Trade ticks (JSONL / stream)
        │
        ▼
MultiTimeframeAggregator (1m, 5m, 1h)
        │
        ▼
ParquetLakeWriter ──► data/{exchange}/{symbol}/{tf}/year=.../month=.../day=.../
        │
        ▼
DuckDBLakeCatalog ──► SQL queries / analytics
```

### Core components

| Module | Responsibility |
|--------|----------------|
| `aggregator.py` | Builds OHLCV candles and rolls buckets forward |
| `writers/parquet_writer.py` | ZSTD-compressed Parquet partitions |
| `storage/duckdb_catalog.py` | DuckDB view over all candle files |
| `ingest/jsonl_source.py` | Replay ticks from JSON Lines |
| `pipeline.py` | End-to-end ingest + flush orchestration |

### Technical decisions

- **Decimal** for prices and volume during aggregation
- **String encoding in Parquet** to preserve financial precision
- **Hive-style partitions** by exchange, symbol, timeframe and date
- **Batch flushing** to reduce I/O overhead under high tick rates

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

### Ingest ticks from JSONL

```bash
market-data-lakehouse ingest \
  --input samples/btcusdt_ticks.jsonl \
  --exchange binance \
  --symbol BTCUSDT \
  --root data/lake
```

### Query materialized candles

```bash
market-data-lakehouse query \
  --root data/lake \
  --symbol BTCUSDT \
  --timeframe 1m \
  --limit 10
```

---

## JSONL tick format

Compatible with normalized `TradeEvent` payloads:

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
    )
    pipeline = MarketDataLakehousePipeline(config)
    await pipeline.run(JsonlTickSource("ticks.jsonl"))


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

## Ecosystem integration

```text
websocket-feed-handler → market-data-lakehouse → ta-indicators-from-scratch
order-book-reconstructor ────────────────────────┘
```

---

## License

MIT
