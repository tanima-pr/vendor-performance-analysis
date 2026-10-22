"""
Tests for the PySpark vendor pipeline.  Run from the repo root:   pytest -q

1. Parity: Spark output == the original pandas/SQLite pipeline on the same data
2. Business-metric edge cases (no sales, zero purchases)
3. Reconciliation catches lost purchase dollars
4. Streaming running totals == batch totals
"""
import os
import sqlite3
import sys

import pandas as pd
import pytest
from pyspark.sql import SparkSession

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "spark"))

import make_sample_data                       # noqa: E402
import stream_sales                           # noqa: E402
import vendor_summary_spark as vs             # noqa: E402
from get_vendor_summary import clean_data, create_vendor_summary  # noqa: E402

KEY = ["VendorNumber", "Brand"]


@pytest.fixture(scope="session")
def spark():
    s = (SparkSession.builder.master("local[2]").appName("tests")
         .config("spark.sql.shuffle.partitions", "4").getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


@pytest.fixture(scope="session")
def sample_dir(tmp_path_factory):
    d = tmp_path_factory.mktemp("sample")
    make_sample_data.main(str(d))
    return str(d)


@pytest.fixture(scope="session")
def spark_summary(spark, sample_dir):
    raw = vs.load_raw(spark, sample_dir)
    return vs.add_metrics(vs.build_summary(*raw)).toPandas()


@pytest.fixture(scope="session")
def pandas_summary(sample_dir):
    """Run the ORIGINAL pandas/SQLite logic on the same sample files."""
    con = sqlite3.connect(":memory:")
    for name in ("purchases", "purchase_prices", "sales", "vendor_invoice"):
        pd.read_csv(os.path.join(sample_dir, f"{name}.csv")).to_sql(name, con, index=False)
    return clean_data(create_vendor_summary(con))


def test_same_rows_and_keys_as_pandas(spark_summary, pandas_summary):
    assert len(spark_summary) == len(pandas_summary)
    sk = set(map(tuple, spark_summary[KEY].astype(int).values.tolist()))
    pk = set(map(tuple, pandas_summary[KEY].astype(int).values.tolist()))
    assert sk == pk


@pytest.mark.parametrize("col", ["TotalPurchaseQuantity", "TotalPurchaseDollars", "TotalSalesQuantity",
                                 "TotalSalesDollars", "TotalExciseTax", "FreightCost", "GrossProfit"])
def test_column_totals_match_pandas(spark_summary, pandas_summary, col):
    assert spark_summary[col].sum() == pytest.approx(pandas_summary[col].sum(), abs=0.01)


def test_row_level_metrics_match_pandas(spark_summary, pandas_summary):
    cols = KEY + ["ProfitMargin", "StockTurnover", "SalesToPurchaseRatio"]
    m = spark_summary[cols].merge(pandas_summary[cols], on=KEY, suffixes=("_spark", "_pandas"))
    for c in ["ProfitMargin", "StockTurnover", "SalesToPurchaseRatio"]:
        assert (m[f"{c}_spark"] - m[f"{c}_pandas"]).abs().max() < 1e-6


def test_brand_never_sold_gets_zero_sales_and_zero_margin(spark):
    df = spark.createDataFrame(
        [(1, "V", 10, "D", 5.0, 7.0, 750.0, 4, 20.0, None, None, None, None, None)],
        "VendorNumber int, VendorName string, Brand int, Description string, PurchasePrice double, "
        "ActualPrice double, Volume double, TotalPurchaseQuantity long, TotalPurchaseDollars double, "
        "TotalSalesQuantity long, TotalSalesDollars double, TotalSalesPrice double, "
        "TotalExciseTax double, FreightCost double")
    row = vs.add_metrics(df).first()
    assert row["TotalSalesDollars"] == 0
    assert row["GrossProfit"] == -20.0
    assert row["ProfitMargin"] == 0.0          # no divide-by-zero
    assert row["StockTurnover"] == 0.0


def test_reconciliation_passes_on_clean_data(spark, sample_dir):
    purchases, prices, sales, invoices = vs.load_raw(spark, sample_dir)
    summary = vs.add_metrics(vs.build_summary(purchases, prices, sales, invoices))
    assert vs.reconcile(summary, purchases, prices)["match"]


def test_reconciliation_catches_missing_rows(spark, sample_dir):
    purchases, prices, sales, invoices = vs.load_raw(spark, sample_dir)
    summary = vs.add_metrics(vs.build_summary(purchases, prices, sales, invoices))
    broken = summary.limit(summary.count() - 1)          # simulate a lost row
    assert not vs.reconcile(broken, purchases, prices)["match"]


def test_streaming_totals_equal_batch_totals(tmp_path, sample_dir):
    sales_csv = os.path.join(sample_dir, "sales.csv")
    landing, out, ckpt = (str(tmp_path / d) for d in ("landing", "out", "ckpt"))
    n = stream_sales.simulate_feed(sales_csv, landing, 4)
    stream_sales.run(landing, out, ckpt, available_now=True)
    totals = pd.read_parquet(os.path.join(out, "running_totals"))
    batch = pd.read_csv(sales_csv)
    assert totals["Transactions"].sum() == n
    assert totals["Revenue"].sum() == pytest.approx(batch["SalesDollars"].sum(), abs=0.01)
