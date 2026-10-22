
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


        # Import every supplier price CSV, including correction files.
        supplier_files = sorted((data_dir / "suppliers").glob("prices_*.csv"))

        if not supplier_files:
            raise FileNotFoundError(
                f"No supplier price CSV files found in {data_dir / 'suppliers'}"
            )

        supplier_frames = []

        for csv_path in supplier_files:
            # Keep source text (including blanks and strings such as "NA") for quarantine.
            frame = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
            frame["source_file"] = csv_path.name
            supplier_frames.append(frame)

        supplier_prices = pd.concat(supplier_frames, ignore_index=True)

        if not {"unit_cost", "cost_sar"}.intersection(supplier_prices.columns):
            raise ValueError("Supplier CSV files must contain unit_cost or cost_sar")
        # Either cost-column generation is valid without the other being present.
        for column in ("unit_cost", "cost_sar", "currency"):
            if column not in supplier_prices.columns:
                supplier_prices[column] = pd.Series(
                    pd.NA, index=supplier_prices.index, dtype="string"
                )

        con.register("supplier_frame", supplier_prices)
        try:
            con.execute(
                "CREATE OR REPLACE TABLE raw.supplier_prices "
                "AS SELECT * FROM supplier_frame"
            )
        finally:
            con.unregister("supplier_frame")

        print(f"Loaded raw.supplier_prices: {len(supplier_prices):,} rows")


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
    """Publish DuckDB models to Iceberg with schema evolution."""

    catalog = get_catalog(warehouse)

    tables = (
        ("silver", "product_cost_history"),
        ("gold", "weekly_margin"),
        ("quality", "price_quarantine"),
    )

    with duckdb.connect(str(db), read_only=True) as con:

        for namespace, table_name in tables:

            identifier = f"{namespace}.{table_name}"

            arrow_table = con.execute(
                f'SELECT * FROM "{namespace}"."{table_name}"'
            ).to_arrow_table()

            if (namespace,) not in catalog.list_namespaces():
                catalog.create_namespace(namespace)

            if catalog.table_exists(identifier):

                iceberg_table = catalog.load_table(identifier)

                # Add new columns without dropping the table.
                with iceberg_table.update_schema() as update:
                    update.union_by_name(arrow_table.schema)

                # Refresh table metadata after schema evolution.
                iceberg_table.refresh()

                # Preserve existing table and snapshot history.
                iceberg_table.overwrite(arrow_table)

                print(f"Updated Iceberg table: {identifier}")

            else:

                iceberg_table = catalog.create_table(
                    identifier=identifier,
                    schema=arrow_table.schema,
                )

                iceberg_table.append(arrow_table)

                print(f"Created Iceberg table: {identifier}")

            print(f"Published rows: {arrow_table.num_rows:,}")


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
