# Spark layer: PySpark, Scala and Structured Streaming

The original pipeline (`ingestion_db.py` → `get_vendor_summary.py`) loads every CSV into
pandas and SQLite. This folder rebuilds the same `vendor_sales_summary` with **Apache Spark**,
so the aggregation runs in parallel instead of in one machine's memory, and adds data-quality
checks, reconciliation, a Scala version and a streaming version.

| File | What it does |
|---|---|
| `vendor_summary_spark.py` | PySpark batch job: reads the 4 raw CSVs, profiles them (row counts, null keys, bad prices), builds the summary with distributed joins and group-bys, reconciles purchase dollars to the cent, writes Parquet, and saves timings to `run_metrics.json` |
| `scala/VendorSummary.scala` | The same job written in Scala (Spark DataFrame API), run in `spark-shell` |
| `stream_sales.py` | Spark Structured Streaming: sales files landing in a folder update running totals per vendor+brand each micro-batch |
| `compare_with_pandas.py` | Proves Spark and pandas give the same answer (rows, keys, column totals) |
| `make_sample_data.py` | Small synthetic copy of the raw files (same columns) for tests and CI |
| `../tests/test_spark_pipeline.py` | 13 pytest tests: pandas parity, metric edge cases, reconciliation, streaming = batch |
| `../docs/data_mapping.md` | Source-to-target mapping for every column, join rule and data-quality check |
| `../.github/workflows/ci.yml` | GitHub Actions: runs pytest, the PySpark job and the Scala job on every push |

## Run it

Needs Java 17 or 21.

```bash
pip install -r requirements-spark.txt

# 1. Quick check on sample data
python spark/make_sample_data.py
python spark/vendor_summary_spark.py --input-dir spark/sample_data --output-dir spark/output
pytest -q

# 2. Full run on the REAL data (folder with purchases.csv, purchase_prices.csv, sales.csv, vendor_invoice.csv)
python spark/vendor_summary_spark.py --input-dir /path/to/raw_csvs --output-dir spark/output_real
python spark/compare_with_pandas.py \
    --spark-output spark/output_real/vendor_sales_summary_parquet \
    --pandas-csv vendor_sales_summary.csv

# 3. Scala version
bash spark/scala/run_scala.sh /path/to/raw_csvs spark/output_scala

# 4. Streaming demo (splits sales.csv into 10 files and streams them through)
python spark/stream_sales.py --sales-csv /path/to/raw_csvs/sales.csv --simulate 10 --available-now
```

## Results on the real data

Run on a MacBook Air (Apple Silicon, Spark 4.0.1 local mode, Java 17):

| Metric | Result |
|---|---|
| Raw rows read | **15.2M**: 12,825,363 sales + 2,372,474 purchases (+ 12,261 prices, 5,543 invoices) |
| Data-quality checks | 0 null keys in any input; 153 zero/negative-price purchase lines filtered out; 0 negative sales |
| Output | **10,692** vendor-brand rows |
| Reconciliation | purchase dollars matched to the cent: **$321,900,765.53** |
| Parity with pandas pipeline | same 10,692 rows, same keys, all 6 column totals identical (`all_match: True`) |
| Run time, raw CSVs to summary | **38 s** with Spark vs **~72 min** for the original pandas + SQLite path (72.2 min loading CSVs into SQLite + 0.26 min aggregating) |
| Scala version | 10,692 rows, reconciliation matched |
| Streaming version | 12,825,363 sales rows processed in 10 micro-batches; running totals updated per batch |

Why Spark is faster here: the original script loads every raw row into SQLite before aggregating;
Spark reads the CSVs in parallel and aggregates them directly, without a row-by-row database load.
