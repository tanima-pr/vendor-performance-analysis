"""
get_vendor_summary.py
---------------------
Rebuilds the `vendor_sales_summary` table from scratch:
  1. Merge purchases + sales + freight + prices into one table (one row per Vendor+Brand)
  2. Clean the data (types, whitespace, missing values)
  3. Add business metrics (GrossProfit, ProfitMargin, StockTurnover, SalesToPurchaseRatio)
  4. Save the finished table back into inventory.db

Run once:  python get_vendor_summary.py
"""
import time
import logging
import sqlite3
import numpy as np
import pandas as pd

logging.basicConfig(
    filename="logs_get_vendor_summary.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a",
)


def create_vendor_summary(conn):
    """Merge the raw tables into one aggregated Vendor+Brand summary (pre-metrics)."""
    query = """
    WITH FreightSummary AS (
        SELECT VendorNumber, SUM(Freight) AS FreightCost
        FROM vendor_invoice
        GROUP BY VendorNumber
    ),
    PurchaseSummary AS (
        SELECT
            p.VendorNumber, p.VendorName, p.Brand, p.Description,
            p.PurchasePrice,
            pp.Price  AS ActualPrice,
            pp.Volume,
            SUM(p.Quantity) AS TotalPurchaseQuantity,
            SUM(p.Dollars)  AS TotalPurchaseDollars
        FROM purchases p
        JOIN purchase_prices pp ON p.Brand = pp.Brand
        WHERE p.PurchasePrice > 0
        GROUP BY p.VendorNumber, p.VendorName, p.Brand, p.Description,
                 p.PurchasePrice, pp.Price, pp.Volume
    ),
    SalesSummary AS (
        SELECT
            VendorNo, Brand,
            SUM(SalesQuantity) AS TotalSalesQuantity,
            SUM(SalesDollars)  AS TotalSalesDollars,
            SUM(SalesPrice)    AS TotalSalesPrice,
            SUM(ExciseTax)     AS TotalExciseTax
        FROM sales
        GROUP BY VendorNo, Brand
    )
    SELECT
        ps.VendorNumber, ps.VendorName, ps.Brand, ps.Description,
        ps.PurchasePrice, ps.ActualPrice, ps.Volume,
        ps.TotalPurchaseQuantity, ps.TotalPurchaseDollars,
        ss.TotalSalesQuantity, ss.TotalSalesDollars, ss.TotalSalesPrice, ss.TotalExciseTax,
        fs.FreightCost
    FROM PurchaseSummary ps
    LEFT JOIN SalesSummary  ss ON ps.VendorNumber = ss.VendorNo AND ps.Brand = ss.Brand
    LEFT JOIN FreightSummary fs ON ps.VendorNumber = fs.VendorNumber
    ORDER BY ps.TotalPurchaseDollars DESC
    """
    return pd.read_sql_query(query, conn)


def clean_data(df):
    """Clean the merged data and add the business metric columns."""
    # 1. Fix data types
    df["Volume"] = pd.to_numeric(df["Volume"], errors="coerce")

    # 2. Missing sales (brands bought but never sold) = 0
    df.fillna(0, inplace=True)

    # 3. Remove hidden whitespace from text columns
    df["VendorName"] = df["VendorName"].str.strip()
    df["Description"] = df["Description"].str.strip()

    # 4. Business metrics (replace 0 with NaN to avoid divide-by-zero, then fill back)
    df["GrossProfit"] = df["TotalSalesDollars"] - df["TotalPurchaseDollars"]
    df["ProfitMargin"] = df["GrossProfit"] / df["TotalSalesDollars"].replace(0, np.nan) * 100
    df["StockTurnover"] = df["TotalSalesQuantity"] / df["TotalPurchaseQuantity"].replace(0, np.nan)
    df["SalesToPurchaseRatio"] = df["TotalSalesDollars"] / df["TotalPurchaseDollars"].replace(0, np.nan)
    df.fillna(0, inplace=True)

    return df


if __name__ == "__main__":
    start = time.time()
    conn = sqlite3.connect("inventory.db")

    logging.info("Creating vendor summary table...")
    summary = create_vendor_summary(conn)
    logging.info(f"Aggregated to {len(summary):,} vendor-brand rows")

    logging.info("Cleaning data and adding metrics...")
    clean_df = clean_data(summary)

    logging.info("Saving vendor_sales_summary to database...")
    clean_df.to_sql("vendor_sales_summary", con=conn, if_exists="replace", index=False)

    conn.close()
    mins = (time.time() - start) / 60
    logging.info(f"Done: {clean_df.shape[0]:,} rows x {clean_df.shape[1]} cols in {mins:.2f} min")
    print(f"Rebuilt vendor_sales_summary: {clean_df.shape} in {mins:.2f} min")
