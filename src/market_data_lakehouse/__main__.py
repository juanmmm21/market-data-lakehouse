from __future__ import annotations

import argparse
import asyncio
import json
import logging

from market_data_lakehouse.ingest.jsonl_source import JsonlTickSource
from market_data_lakehouse.models import ExchangeId, LakehouseConfig, Timeframe
from market_data_lakehouse.pipeline import MarketDataLakehousePipeline
from market_data_lakehouse.storage.duckdb_catalog import DuckDBLakeCatalog
from market_data_lakehouse.writers.parquet_writer import ParquetLakeWriter


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Pipeline local de velas OHLCV con persistencia Parquet y consultas DuckDB.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    ingest = subparsers.add_parser("ingest", help="Ingesta ticks JSONL y genera velas.")
    ingest.add_argument("--input", required=True, help="Archivo JSON Lines de ticks.")
    ingest.add_argument("--root", default="data/lake", help="Directorio raíz del lakehouse.")
    ingest.add_argument("--exchange", choices=[item.value for item in ExchangeId], required=True)
    ingest.add_argument("--symbol", required=True)
    ingest.add_argument(
        "--timeframes",
        nargs="+",
        default=[Timeframe.M1.value, Timeframe.M5.value, Timeframe.H1.value],
        choices=[item.value for item in Timeframe],
    )
    ingest.add_argument("--duckdb-path", default=None)
    ingest.add_argument("--flush-batch-size", type=int, default=50)

    query = subparsers.add_parser("query", help="Consulta velas materializadas con DuckDB.")
    query.add_argument("--root", default="data/lake")
    query.add_argument("--duckdb-path", default=None)
    query.add_argument("--symbol", required=True)
    query.add_argument("--timeframe", required=True, choices=[item.value for item in Timeframe])
    query.add_argument("--limit", type=int, default=20)

    return parser


async def _run_ingest(args: argparse.Namespace) -> None:
    timeframes = tuple(Timeframe(value) for value in args.timeframes)
    config = LakehouseConfig(
        root_path=args.root,
        exchange=ExchangeId(args.exchange),
        symbol=args.symbol,
        timeframes=timeframes,
        duckdb_path=args.duckdb_path,
        flush_batch_size=args.flush_batch_size,
    )
    source = JsonlTickSource(args.input)
    pipeline = MarketDataLakehousePipeline(config)
    processed = await pipeline.run(source)
    logging.getLogger(__name__).info("ingest completed ticks=%s symbol=%s", processed, args.symbol)


def _run_query(args: argparse.Namespace) -> None:
    writer = ParquetLakeWriter(args.root)
    duckdb_path = args.duckdb_path or f"{args.root.rstrip('/')}/catalog.duckdb"
    catalog = DuckDBLakeCatalog(duckdb_path=duckdb_path, parquet_writer=writer)
    try:
        rows = catalog.query_candles(args.symbol, args.timeframe, limit=args.limit)
        print(json.dumps(rows, default=str, indent=2))
    finally:
        catalog.close()


def main() -> None:
    args = build_parser().parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )

    if args.command == "ingest":
        asyncio.run(_run_ingest(args))
        return

    if args.command == "query":
        _run_query(args)
        return

    raise RuntimeError(f"unsupported command: {args.command}")


if __name__ == "__main__":
    main()
