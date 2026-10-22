"""
stream_sales.py
---------------
Spark Structured Streaming demo on the sales data: new sales files land in a folder,
and Spark keeps a running total of units, revenue and excise tax per vendor+brand.

To simulate a live feed, `--simulate N` first splits sales.csv into N smaller files and
drops them into the landing folder, which Spark then picks up as separate micro-batches.

  # process everything that's in the landing folder, then stop (good for tests/CI)
  python spark/stream_sales.py --sales-csv spark/sample_data/sales.csv --simulate 5 --available-now

  # keep running and watch the folder (drop new CSVs into spark/stream/landing to see totals update)
  python spark/stream_sales.py --sales-csv spark/sample_data/sales.csv --simulate 5

Results are written each micro-batch to spark/stream/running_totals (Parquet, overwritten),
and a per-batch log goes to spark/stream/batch_log.jsonl.
"""
import argparse
import json
import os
import shutil
import time

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, IntegerType, LongType, StringType, StructField,
                               StructType)

# Streaming sources need an explicit schema (no inference on a live feed).
SALES_SCHEMA = StructType([
    StructField("InventoryId", StringType()), StructField("Store", IntegerType()),
    StructField("Brand", IntegerType()), StructField("Description", StringType()),
    StructField("Size", StringType()), StructField("SalesQuantity", LongType()),
    StructField("SalesDollars", DoubleType()), StructField("SalesPrice", DoubleType()),
    StructField("SalesDate", StringType()), StructField("Volume", StringType()),
    StructField("Classification", IntegerType()), StructField("ExciseTax", DoubleType()),
    StructField("VendorNo", IntegerType()), StructField("VendorName", StringType()),
])


def simulate_feed(sales_csv, landing_dir, n_files):
    """Split the sales file into n_files chunks to mimic files arriving over time."""
    shutil.rmtree(landing_dir, ignore_errors=True)
    os.makedirs(landing_dir)
    df = pd.read_csv(sales_csv)
    size = -(-len(df) // n_files)  # ceiling division
    for i in range(n_files):
        df.iloc[i * size:(i + 1) * size].to_csv(
            os.path.join(landing_dir, f"sales_part_{i:03d}.csv"), index=False)
    return len(df)


def run(landing_dir, out_dir, checkpoint_dir, available_now=True):
    spark = (SparkSession.builder.appName("sales-stream")
             .config("spark.sql.shuffle.partitions", "4").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    stream = (spark.readStream.option("header", True).option("maxFilesPerTrigger", 1)
              .schema(SALES_SCHEMA).csv(landing_dir))

    running = (stream.filter(F.col("Brand").isNotNull())
               .groupBy("VendorNo", "Brand")
               .agg(F.sum("SalesQuantity").alias("UnitsSold"),
                    F.sum("SalesDollars").alias("Revenue"),
                    F.sum("ExciseTax").alias("ExciseTax"),
                    F.count(F.lit(1)).alias("Transactions")))

    log_path = os.path.join(out_dir, "batch_log.jsonl")
    os.makedirs(out_dir, exist_ok=True)

    def write_batch(batch_df, batch_id):
        # outputMode("complete") hands us the full, updated totals table every micro-batch
        batch_df.write.mode("overwrite").parquet(os.path.join(out_dir, "running_totals"))
        tot = batch_df.agg(F.sum("Revenue").alias("rev"), F.sum("Transactions").alias("tx")).first()
        with open(log_path, "a") as f:
            f.write(json.dumps({"batch_id": batch_id, "ts": time.strftime("%H:%M:%S"),
                                "vendor_brands": batch_df.count(),
                                "transactions_so_far": tot["tx"],
                                "revenue_so_far": round(tot["rev"] or 0, 2)}) + "\n")

    writer = (running.writeStream.outputMode("complete").foreachBatch(write_batch)
              .option("checkpointLocation", checkpoint_dir))
    query = (writer.trigger(availableNow=True) if available_now
             else writer.trigger(processingTime="10 seconds")).start()
    query.awaitTermination()
    spark.stop()


if __name__ == "__main__":
    here = os.path.dirname(os.path.abspath(__file__))
    base = os.path.join(here, "stream")
    ap = argparse.ArgumentParser()
    ap.add_argument("--sales-csv", default=os.path.join(here, "sample_data", "sales.csv"))
    ap.add_argument("--simulate", type=int, default=5, help="split sales.csv into N files first (0 = skip)")
    ap.add_argument("--available-now", action="store_true", help="process what's there, then stop")
    a = ap.parse_args()

    landing, out, ckpt = (os.path.join(base, d) for d in ("landing", "output", "checkpoint"))
    if a.simulate:
        shutil.rmtree(out, ignore_errors=True)
        shutil.rmtree(ckpt, ignore_errors=True)
        rows = simulate_feed(a.sales_csv, landing, a.simulate)
        print(f"Simulated feed: {rows:,} sales rows split into {a.simulate} files in {landing}")
    run(landing, out, ckpt, available_now=a.available_now)
    with open(os.path.join(out, "batch_log.jsonl")) as f:
        for line in f:
            print(line.strip())
