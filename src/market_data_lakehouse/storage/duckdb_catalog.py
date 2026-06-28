from __future__ import annotations

import logging
from pathlib import Path

import duckdb

from market_data_lakehouse.writers.parquet_writer import ParquetLakeWriter

logger = logging.getLogger(__name__)


class DuckDBLakeCatalog:
    """Catálogo analítico DuckDB sobre archivos Parquet del lakehouse."""

    def __init__(self, duckdb_path: str | Path, parquet_writer: ParquetLakeWriter) -> None:
        self._duckdb_path = Path(duckdb_path)
        self._parquet_writer = parquet_writer
        self._duckdb_path.parent.mkdir(parents=True, exist_ok=True)
        self._connection = duckdb.connect(str(self._duckdb_path))
        self._register_views()

    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        return self._connection

    def _register_views(self) -> None:
        files = [str(path) for path in self._parquet_writer.root_path.rglob("candles.parquet")]
        if not files:
            logger.warning(
                "no parquet files found yet for duckdb catalog root=%s",
                self._parquet_writer.root_path,
            )
            self._create_empty_view()
            return

        paths_sql = ", ".join(self._quote_path(path) for path in files)
        try:
            self._connection.execute(
                f"""
                CREATE OR REPLACE VIEW candles AS
                SELECT *
                FROM read_parquet([{paths_sql}], union_by_name=true)
                """,
            )
        except duckdb.Error as exc:
            logger.exception("failed to register duckdb view over parquet files")
            raise RuntimeError("failed to register duckdb parquet view") from exc

    def _create_empty_view(self) -> None:
        self._connection.execute(
            """
            CREATE OR REPLACE VIEW candles AS
            SELECT
                CAST(NULL AS VARCHAR) AS exchange,
                CAST(NULL AS VARCHAR) AS symbol,
                CAST(NULL AS VARCHAR) AS timeframe,
                CAST(NULL AS TIMESTAMPTZ) AS open_time,
                CAST(NULL AS TIMESTAMPTZ) AS close_time,
                CAST(NULL AS VARCHAR) AS open,
                CAST(NULL AS VARCHAR) AS high,
                CAST(NULL AS VARCHAR) AS low,
                CAST(NULL AS VARCHAR) AS close,
                CAST(NULL AS VARCHAR) AS volume,
                CAST(NULL AS BIGINT) AS trade_count
            WHERE 1 = 0
            """,
        )

    @staticmethod
    def _quote_path(path: str) -> str:
        return "'" + path.replace("'", "''") + "'"

    def refresh(self) -> None:
        self._register_views()

    def query_candles(
        self,
        symbol: str,
        timeframe: str,
        *,
        limit: int = 100,
    ) -> list[dict[str, object]]:
        if limit <= 0:
            raise ValueError("limit must be positive")

        self.refresh()
        result = self._connection.execute(
            """
            SELECT *
            FROM candles
            WHERE symbol = ? AND timeframe = ?
            ORDER BY open_time DESC
            LIMIT ?
            """,
            [symbol, timeframe, limit],
        )
        columns = [column[0] for column in result.description]
        rows = result.fetchall()
        return [dict(zip(columns, row, strict=True)) for row in rows]

    def close(self) -> None:
        self._connection.close()
