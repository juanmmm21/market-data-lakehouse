# market-data-lakehouse

Pipeline local de **data lake** que agrega ticks en tiempo real en velas OHLCV (`1m`, `5m`, `1h`), las persiste en particiones **Parquet** y expone un catálogo **DuckDB** para consultas analíticas. Tercer módulo del ecosistema [quant-core-infra](https://github.com/juanmmm21/quant-core-infra).

Repositorio: [github.com/juanmmm21/market-data-lakehouse](https://github.com/juanmmm21/market-data-lakehouse)

---

## Qué es y qué problema resuelve

Los ticks individuales son demasiado granulares para análisis técnico, backtesting por velas o almacenamiento eficiente a largo plazo. Este módulo transforma millones de trades en **velas OHLCV** con ventanas temporales fijas y las guarda en un formato columnar comprimido (Parquet) consultable con SQL (DuckDB).

Resuelve tres necesidades a la vez:

1. **Agregación en streaming** — construir velas sobre el fly sin esperar al cierre del bucket
2. **Persistencia analítica** — Parquet particionado por exchange, símbolo, timeframe y fecha
3. **Replay offline** — ingestar JSONL de ticks para reproducir históricos sin WebSocket

---

## Rol en quant-core-infra

```text
websocket-feed-handler ──► ticks ──► market-data-lakehouse ──► velas OHLCV
                                           │
                                           ├──► ta-indicators-from-scratch
                                           └──► alpha-signal-generator (vía JSONL)
```

Es el **almacén local de series temporales** del ecosistema. Los indicadores técnicos y las estrategias consumen sus velas, no los ticks crudos.

---

## Objetivo

Demuestra:

- Agregación OHLCV multi-timeframe desde ticks normalizados
- Almacenamiento particionado optimizado para analítica
- Catálogo DuckDB sobre archivos Parquet
- Ingesta JSONL compatible con `websocket-feed-handler`

---

## Cómo funciona

1. **Ingesta:** `JsonlTickSource` lee ticks línea a línea (o un stream async futuro).
2. **Agregación:** `MultiTimeframeAggregator` mantiene un bucket activo por timeframe (`1m`, `5m`, `1h`).
3. **Por cada tick:** se actualiza `open/high/low/close/volume/trade_count` del bucket actual.
4. **Roll forward:** cuando el tick cae en un bucket nuevo, la vela cerrada se emite al writer.
5. **Escritura:** `ParquetLakeWriter` acumula velas en batch y las escribe en particiones Hive-style.
6. **Catálogo:** `DuckDBLakeCatalog` registra las rutas Parquet y permite SQL directo.

### Estructura de particiones

```text
data/lake/
└── binance/
    └── BTCUSDT/
        ├── 1m/year=2024/month=01/day=01/part-000.parquet
        ├── 5m/year=2024/month=01/day=01/part-000.parquet
        └── 1h/year=2024/month=01/day=01/part-000.parquet
```

---

## Arquitectura

```text
Trade ticks (JSONL / stream)
        │
        ▼
MultiTimeframeAggregator (1m, 5m, 1h)
        │
        ▼
ParquetLakeWriter ──► particiones ZSTD
        │
        ▼
DuckDBLakeCatalog ──► SELECT ... FROM candles
```

### Componentes

| Módulo | Responsabilidad |
|--------|----------------|
| `aggregator.py` | Construye velas y hace roll de buckets |
| `timeframes.py` | Alineación de buckets UTC |
| `writers/parquet_writer.py` | Escritura Parquet comprimida |
| `storage/duckdb_catalog.py` | Vista SQL sobre todos los archivos |
| `ingest/jsonl_source.py` | Replay desde JSON Lines |
| `pipeline.py` | Orquestación ingest + flush |

### Decisiones técnicas

- **Decimal** durante agregación; strings en Parquet para preservar precisión
- **Particiones Hive** por exchange, símbolo, timeframe y fecha
- **Batch flush** configurable para reducir I/O bajo alto tick rate
- **Timezone-aware** — todos los buckets en UTC

---

## Modelo de datos

### Tick de entrada (`TradeTick`)

| Campo | Tipo | Notas |
|-------|------|-------|
| `exchange` | `binance` / `coinbase` | |
| `symbol` | `str` | |
| `price`, `quantity` | `Decimal` | |
| `side` | `buy` / `sell` | |
| `event_time` | `datetime` UTC | |

### Vela de salida (`Candle`)

| Campo | Descripción |
|-------|-------------|
| `open`, `high`, `low`, `close` | OHLC en Decimal |
| `volume` | Suma de cantidades |
| `trade_count` | Número de ticks en la vela |
| `open_time`, `close_time` | Límites del bucket |

---

## Requisitos

- Python **3.11+**

---

## Instalación

```bash
cd market-data-lakehouse
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

---

## Uso CLI

### Ingestar ticks

```bash
market-data-lakehouse ingest \
  --input samples/btcusdt_ticks.jsonl \
  --exchange binance \
  --symbol BTCUSDT \
  --root data/lake \
  --timeframes 1m 5m 1h \
  --flush-batch-size 50
```

### Consultar velas

```bash
market-data-lakehouse query \
  --root data/lake \
  --symbol BTCUSDT \
  --timeframe 1m \
  --limit 10
```

---

## Formato JSONL de ticks

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

## Uso programático

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

## Desarrollo

```bash
pytest -q
ruff check src tests
mypy src
```

---

## Roadmap

- [ ] Ingesta directa desde `websocket-feed-handler` sin JSONL intermedio
- [ ] Soporte timeframes adicionales (`15m`, `4h`, `1d`)
- [ ] Compaction automática de particiones pequeñas

---

## Licencia

MIT
