"""
vendor_summary_spark.py
-----------------------
PySpark version of get_vendor_summary.py. Same business logic, but built for scale:
Spark reads the raw CSVs in parallel, aggregates with distributed group-bys instead of
loading everything into pandas/SQLite, and writes the result as Parquet.

Steps
  1. Read the 4 raw CSVs (purchases, purchase_prices, sales, vendor_invoice)
  2. Data-quality checks on the raw inputs (row counts, null keys, negative values)
  3. Build FreightSummary / PurchaseSummary / SalesSummary and join them (same as the SQL CTEs)
  4. Clean + add business metrics (GrossProfit, ProfitMargin, StockTurnover, SalesToPurchaseRatio)
  5. Reconcile totals against the raw data, then write Parquet (+ one CSV for easy viewing)
  6. Save run metrics (row counts, timings, checks) to run_metrics.json

Run (real data):    python spark/vendor_summary_spark.py --input-dir /path/to/raw_csvs --output-dir spark/output
Run (sample data):  python spark/make_sample_data.py && python spark/vendor_summary_spark.py --input-dir spark/sample_data
"""
import argparse
import json
import os
import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def get_spark(app_name="vendor-summary"):
    return (SparkSession.builder.appName(app_name)
            .config("spark.sql.shuffle.partitions", "8")   # small cluster / laptop friendly
            .getOrCreate())


def quiet(spark):
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def num(col, dtype):
    """Tolerant cast: bad values (e.g. Volume = 'Unknown') become NULL instead of failing,
    like pd.to_numeric(errors="coerce"). Works on Spark 3.5 and 4.x (ANSI mode)."""
    return F.expr(f"try_cast(`{col}` as {dtype})").alias(col)


def read_csv(spark, path):
    return spark.read.option("header", True).csv(path)


def load_raw(spark, input_dir):
    """Read only the columns the pipeline needs and cast them to the right types."""
    p = os.path.join
    purchases = read_csv(spark, p(input_dir, "purchases.csv")).select(
        num("VendorNumber", "int"), "VendorName", num("Brand", "int"), "Description",
        num("PurchasePrice", "double"), num("Quantity", "long"),
        num("Dollars", "double"))
    prices = read_csv(spark, p(input_dir, "purchase_prices.csv")).select(
        num("Brand", "int"), num("Price", "double").alias("ActualPrice"),
        num("Volume", "double"))  # "Unknown" -> null, like pd.to_numeric(errors="coerce")
    sales = read_csv(spark, p(input_dir, "sales.csv")).select(
        num("VendorNo", "int"), num("Brand", "int"),
        num("SalesQuantity", "long"), num("SalesDollars", "double"),
        num("SalesPrice", "double"), num("ExciseTax", "double"))
    invoices = read_csv(spark, p(input_dir, "vendor_invoice.csv")).select(
        num("VendorNumber", "int"), num("Freight", "double"))
    return purchases, prices, sales, invoices


def raw_quality_checks(purchases, prices, sales, invoices):
    """Profile the raw inputs. Returns a dict that is saved in run_metrics.json."""
    checks = {}
    for name, df, key in [("purchases", purchases, "Brand"), ("purchase_prices", prices, "Brand"),
                          ("sales", sales, "Brand"), ("vendor_invoice", invoices, "VendorNumber")]:
        row = df.agg(F.count(F.lit(1)).alias("rows"),
                     F.sum(F.col(key).isNull().cast("int")).alias("null_keys")).first()
        checks[name] = {"rows": row["rows"], "null_keys": row["null_keys"] or 0}
    checks["purchases"]["zero_or_negative_price_rows"] = purchases.filter(F.col("PurchasePrice") <= 0).count()
    checks["sales"]["negative_quantity_rows"] = sales.filter(F.col("SalesQuantity") < 0).count()
    return checks


def build_summary(purchases: DataFrame, prices: DataFrame, sales: DataFrame,
                  invoices: DataFrame) -> DataFrame:
    """Same logic as the SQL CTEs in get_vendor_summary.py."""
    freight = invoices.groupBy("VendorNumber").agg(F.sum("Freight").alias("FreightCost"))

    purchase_summary = (purchases.filter(F.col("PurchasePrice") > 0)
                        .join(prices, on="Brand", how="inner")
                        .groupBy("VendorNumber", "VendorName", "Brand", "Description",
                                 "PurchasePrice", "ActualPrice", "Volume")
                        .agg(F.sum("Quantity").alias("TotalPurchaseQuantity"),
                             F.sum("Dollars").alias("TotalPurchaseDollars")))

    sales_summary = (sales.groupBy("VendorNo", "Brand")
                     .agg(F.sum("SalesQuantity").alias("TotalSalesQuantity"),
                          F.sum("SalesDollars").alias("TotalSalesDollars"),
                          F.sum("SalesPrice").alias("TotalSalesPrice"),
                          F.sum("ExciseTax").alias("TotalExciseTax")))

    joined = (purchase_summary.alias("ps")
              .join(sales_summary.alias("ss"),
                    (F.col("ps.VendorNumber") == F.col("ss.VendorNo")) & (F.col("ps.Brand") == F.col("ss.Brand")),
                    "left")
              .join(freight.alias("fs"), F.col("ps.VendorNumber") == F.col("fs.VendorNumber"), "left")
              .select("ps.*", "ss.TotalSalesQuantity", "ss.TotalSalesDollars", "ss.TotalSalesPrice",
                      "ss.TotalExciseTax", "fs.FreightCost"))
    return joined


def add_metrics(df: DataFrame) -> DataFrame:
    """Clean + business metrics, matching clean_data() in get_vendor_summary.py."""
    numeric = ["Volume", "TotalSalesQuantity", "TotalSalesDollars", "TotalSalesPrice",
               "TotalExciseTax", "FreightCost"]
    df = (df.fillna(0, subset=numeric)
            .withColumn("VendorName", F.trim("VendorName"))
            .withColumn("Description", F.trim("Description")))

    def safe_div(num, den):
        return F.when(F.col(den) != 0, F.col(num) / F.col(den)).otherwise(F.lit(0.0))

    return (df.withColumn("GrossProfit", F.col("TotalSalesDollars") - F.col("TotalPurchaseDollars"))
              .withColumn("ProfitMargin", F.when(F.col("TotalSalesDollars") != 0,
                                                 F.col("GrossProfit") / F.col("TotalSalesDollars") * 100)
                                           .otherwise(F.lit(0.0)))
              .withColumn("StockTurnover", safe_div("TotalSalesQuantity", "TotalPurchaseQuantity"))
              .withColumn("SalesToPurchaseRatio", safe_div("TotalSalesDollars", "TotalPurchaseDollars"))
              .orderBy(F.col("TotalPurchaseDollars").desc()))


def reconcile(summary: DataFrame, purchases: DataFrame, prices: DataFrame) -> dict:
    """Total purchase dollars in the summary must equal the qualifying raw purchases."""
    expected = (purchases.filter(F.col("PurchasePrice") > 0)
                .join(prices.select("Brand"), "Brand", "inner")
                .agg(F.sum("Dollars")).first()[0]) or 0.0
    actual = summary.agg(F.sum("TotalPurchaseDollars")).first()[0] or 0.0
    return {"expected_purchase_dollars": round(expected, 2),
            "summary_purchase_dollars": round(actual, 2),
            "match": abs(expected - actual) < 0.01}


def run(input_dir, output_dir):
    t0 = time.time()
    spark = quiet(get_spark())
    purchases, prices, sales, invoices = load_raw(spark, input_dir)
    checks = raw_quality_checks(purchases, prices, sales, invoices)
    t_read = time.time()

    summary = add_metrics(build_summary(purchases, prices, sales, invoices)).cache()
    n_rows = summary.count()
    t_build = time.time()

    recon = reconcile(summary, purchases, prices)
    if not recon["match"]:
        raise ValueError(f"Reconciliation failed: {recon}")

    os.makedirs(output_dir, exist_ok=True)
    summary.write.mode("overwrite").parquet(os.path.join(output_dir, "vendor_sales_summary_parquet"))
    summary.coalesce(1).write.mode("overwrite").option("header", True).csv(
        os.path.join(output_dir, "vendor_sales_summary_csv"))
    t_end = time.time()

    metrics = {
        "spark_version": spark.version,
        "input_dir": input_dir,
        "raw_checks": checks,
        "summary_rows": n_rows,
        "reconciliation": recon,
        "seconds": {"read_and_profile": round(t_read - t0, 2),
                    "build_summary": round(t_build - t_read, 2),
                    "write_outputs": round(t_end - t_build, 2),
                    "total": round(t_end - t0, 2)},
    }
    with open(os.path.join(output_dir, "run_metrics.json"), "w") as f:
        json.dump(metrics, f, indent=2)
    print(json.dumps(metrics, indent=2))
    spark.stop()
    return metrics


if __name__ == "__main__":
    here = os.path.dirname(os.path.abspath(__file__))
    ap = argparse.ArgumentParser()
    ap.add_argument("--input-dir", default=os.path.join(here, "sample_data"))
    ap.add_argument("--output-dir", default=os.path.join(here, "output"))
    a = ap.parse_args()
    run(a.input_dir, a.output_dir)
