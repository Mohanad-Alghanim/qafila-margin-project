
"""Qafila gross margin pipeline: raw files -> DuckDB -> dbt -> Iceberg.

    uv run python run.py --data-dir data --warehouse warehouse

Must work on an empty warehouse folder, and must be safe to run again on the same one.
The grader runs it on a dataset you haven't seen, so never hard-code file names, row counts or SKUs.
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import subprocess
import sys
from pathlib import Path

import duckdb
import pandas as pd
from pyiceberg.catalog.sql import SqlCatalog

DBT_DIR = Path(__file__).resolve().parent / "dbt"


def get_catalog(warehouse: Path) -> SqlCatalog:
    """The Iceberg catalog the grader reads. Don't change the name, the URI or the warehouse."""
    warehouse.mkdir(parents=True, exist_ok=True)
    return SqlCatalog(
        "qafila",
        uri=f"sqlite:///{(warehouse / 'catalog.db').as_posix()}",
        warehouse=warehouse.resolve().as_posix(),
    )


def ingest(data_dir: Path, db: Path) -> None:
    """Step 1: land the sources in DuckDB, schema `raw`.

    - From data_dir/qafila_oltp.sqlite: orders, order_items, products, stores.
    - Supplier CSV ingestion will be added next.

    Running this twice must not duplicate anything.
    """
    db.parent.mkdir(parents=True, exist_ok=True)
    source_db = data_dir / "qafila_oltp.sqlite"

    if not source_db.is_file():
        raise FileNotFoundError(f"SQLite database not found: {source_db}")

    with duckdb.connect(str(db)) as con:
        con.sql("CREATE SCHEMA IF NOT EXISTS raw")

        with sqlite3.connect(source_db) as source:
            for table in ("orders", "order_items", "products", "stores"):
                cursor = source.execute(f'SELECT * FROM "{table}"')
                columns = [item[0] for item in cursor.description]
                rows = cursor.fetchall()

                frame = pd.DataFrame(rows, columns=columns)

                con.register("source_frame", frame)
                try:
                    con.execute(
                        f'CREATE OR REPLACE TABLE raw."{table}" AS '
                        "SELECT * FROM source_frame"
                    )
                finally:
                    con.unregister("source_frame")

                print(f"Loaded raw.{table}: {len(rows):,} rows")


def transform(db: Path) -> None:
    """Steps 2-4: build and test the dbt project in dbt/ against the DuckDB file.

    dbt runs in its own process: in this one it would keep the DuckDB file open and block publish().
    """
    env = {**os.environ, "QAFILA_DUCKDB": str(db.resolve())}
    cmd = [
        sys.executable,
        "-c",
        "from dbt.cli.main import cli; cli()",
        "build",
        "--project-dir",
        str(DBT_DIR),
        "--profiles-dir",
        str(DBT_DIR),
    ]
    if subprocess.run(cmd, env=env).returncode != 0:
        raise SystemExit("dbt build failed")


def publish(db: Path, warehouse: Path) -> None:
    """Step 5: publish three dbt models to Iceberg.

        silver.product_cost_history
        gold.weekly_margin
        quality.price_quarantine

    Update existing tables in place, never drop and recreate them.
    """
    catalog = get_catalog(warehouse)
    raise NotImplementedError("Step 5: publish the models to Iceberg")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--data-dir", type=Path, default=Path("data"))
    ap.add_argument("--warehouse", type=Path, default=Path("warehouse"))
    args = ap.parse_args()

    args.warehouse.mkdir(parents=True, exist_ok=True)
    db = args.warehouse / "qafila.duckdb"
    ingest(args.data_dir, db)
    transform(db)
    publish(db, args.warehouse)
    print(f"done: {args.warehouse.resolve()}")


if __name__ == "__main__":
    main()
