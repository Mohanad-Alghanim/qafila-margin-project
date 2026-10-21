# Qafila gross margin

Submitted by: Mohanad Alghanim — academy: @SDAIAAcademy

The task, rules and grading are in [BRIEF.md](BRIEF.md).

```bash
uv sync
uv run python tools/make_data.py
uv run python run.py
uv run python tools/check.py
```

## Design

**Flow:** SQLite and supplier CSV files → DuckDB `raw` → dbt `staging` → `silver` cost history and `quality` quarantine → `gold` weekly margin → Apache Iceberg.

| Model | Layer | Grain (one row per ...) | Materialization |
|---|---|---|---|
| `stg_supplier_prices` | staging | Supplier price record | View |
| `product_cost_history` | silver | SKU and cost-change effective date | Table |
| `weekly_margin` | gold | ISO week, city and product category | Table |
| `price_quarantine` | quality | Rejected supplier price record | Table |

**How the SCD2 history is built:** Supplier costs are normalized from text, including Arabic-Indic digits and decimal commas. Rows with unknown SKUs or non-positive costs are quarantined. Duplicate SKU and effective-date records are resolved by sorting the source filename so the last filename wins. Missing weekly quotes retain the previous cost, and a new SCD2 version is created only when the cost changes. `valid_to` is the next version's `valid_from`, exclusive.

**How each order line finds its cost:** Delivered order lines with positive quantities and Riyadh-local order dates on or after 2026-08-31 are joined to the cost version valid on the order date. Revenue is rounded per line. Uncosted lines remain in line counts and revenue, but do not contribute to costed revenue or COGS.

## Data problems I found

| Problem | Where | Rows affected | What I did |
|---|---|---:|---|
| Arabic-Indic digits in supplier costs | Supplier CSVs | 34 | Translate digits before numeric conversion |
| Decimal commas in supplier costs | Supplier CSVs | 126 | Normalize commas before casting |
| Rows containing Arabic digits or commas | Supplier CSVs | 160 | Normalize values in staging; this is an overlapping-pattern count, not a rejected-row count |
| Non-positive supplier costs or invalid costs | `price_quarantine` | 4 | Quarantine with reason `invalid_cost` |
| Unknown product SKUs | `price_quarantine` | 3 | Quarantine with reason `unknown_sku` |
| Product SKUs with surrounding whitespace | `raw.products` | 2 | Trim SKUs before matching |
| Order-item rows with non-positive quantities | `raw.order_items` | 142 | Exclude from eligible sales lines |

The current dataset contains 649 supplier price rows after adding the late correction. The quarantine contains 7 rows: 4 `invalid_cost` and 3 `unknown_sku`.

## Tests

| Test | On | What it protects against |
|---|---|---|
| `not_null_product_cost_history_sku` | Silver | Missing SKU keys |
| `not_null_product_cost_history_unit_cost` | Silver | Missing historical costs |
| `not_null_product_cost_history_valid_from` | Silver | Missing effective dates |
| `assert_positive_product_cost` | Silver | Non-positive costs entering history |
| `assert_unique_product_cost_version` | Silver | Duplicate SKU/effective-date versions |
| `assert_valid_cost_intervals` | Silver | Invalid or overlapping SCD2 intervals |

The dbt build completed successfully with all 6 data tests passing. The official contract checker passed **22/22 checks** after the correction was applied.

## Re-runs and the correction

Running the pipeline twice on the same input, before adding the correction, produced identical SHA-256 content hashes for all three Iceberg tables:

| Table | Rows | SHA-256 |
|---|---:|---|
| `silver.product_cost_history` | 633 | `91ae07166b5eee3393bd9da289256fc2d2dc54049d188855d0a1adaaedbcdfb9` |
| `gold.weekly_margin` | 165 | `2203557b940e67b62aefa5b49c309635d85cdd3e23a52fcc4c026bbaa7770eb0` |
| `quality.price_quarantine` | 7 | `b1f782e04d70a0e9df21ed022c1db6b971432b4760efd5f5b84b349670d21d09` |

This confirms that re-running against unchanged inputs did not change table contents.

After generating `prices_2026-W39_fix.csv` and re-running the pipeline, the tables were updated in place. The cost history grew to 637 rows, the weekly margin table remained at 165 rows, and the quarantine remained at 7 rows.

| State | Snapshot ID | Total margin (SAR) |
|---|---|---:|
| Before correction | `575943942737170454` | 801,016.88 |
| After correction | `4340354104184229172` | 803,515.80 |
| Difference | — | +2,498.92 |

The pre-correction snapshot remained readable after the update, demonstrating Iceberg time travel. The official checker passed 22/22 checks after the correction.

## What I'd do next

For larger volumes, I would replace full-table reprocessing with incremental ingestion and partitioning suited to query patterns. For daily supplier files, I would add explicit ingestion metadata, stronger duplicate-file handling, and automated alerts for quarantined records. I would also expand dbt tests to cover weekly margin reconciliation and source freshness.

## AI assistance

ChatGPT was used as a coding and debugging assistant to help investigate dbt model behaviour, DuckDB queries, Iceberg schema evolution and snapshot history, and to interpret validation output. The implementation and final submission should be reviewed by the author.
