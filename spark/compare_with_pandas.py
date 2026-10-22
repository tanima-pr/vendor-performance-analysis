"""
compare_with_pandas.py
----------------------
Proves the Spark pipeline gives the SAME answer as the original pandas/SQLite pipeline.

Compares Spark's Parquet output with the pandas output (vendor_sales_summary.csv):
  - same number of rows
  - same set of (VendorNumber, Brand) keys
  - same column totals for the key money/quantity columns (to the cent)

    python spark/compare_with_pandas.py \
        --spark-output spark/output/vendor_sales_summary_parquet \
        --pandas-csv vendor_sales_summary.csv
"""
import argparse
import sys

import pandas as pd

KEY = ["VendorNumber", "Brand"]
TOTALS = ["TotalPurchaseQuantity", "TotalPurchaseDollars", "TotalSalesQuantity",
          "TotalSalesDollars", "TotalExciseTax", "GrossProfit"]


def compare(spark_df: pd.DataFrame, pandas_df: pd.DataFrame) -> dict:
    result = {"spark_rows": len(spark_df), "pandas_rows": len(pandas_df)}
    sk = set(map(tuple, spark_df[KEY].astype(int).values.tolist()))
    pk = set(map(tuple, pandas_df[KEY].astype(int).values.tolist()))
    result["keys_only_in_spark"] = len(sk - pk)
    result["keys_only_in_pandas"] = len(pk - sk)
    diffs = {}
    for c in TOTALS:
        s, p = float(spark_df[c].sum()), float(pandas_df[c].sum())
        diffs[c] = {"spark": round(s, 2), "pandas": round(p, 2), "match": abs(s - p) < 0.01}
    result["column_totals"] = diffs
    result["all_match"] = (result["spark_rows"] == result["pandas_rows"]
                           and not result["keys_only_in_spark"] and not result["keys_only_in_pandas"]
                           and all(d["match"] for d in diffs.values()))
    return result


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--spark-output", required=True)
    ap.add_argument("--pandas-csv", required=True)
    a = ap.parse_args()
    res = compare(pd.read_parquet(a.spark_output), pd.read_csv(a.pandas_csv))
    for k, v in res.items():
        print(f"{k}: {v}")
    sys.exit(0 if res["all_match"] else 1)
