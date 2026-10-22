# Source-to-Target Data Mapping: `vendor_sales_summary`

Target: **`vendor_sales_summary`**, one row per vendor + brand (+ description, purchase price,
list price and volume). Built by `get_vendor_summary.py` (pandas/SQLite) and
`spark/vendor_summary_spark.py` (PySpark), both of which follow this mapping.

## Source files

| Source file | Grain | Rows (real data) | Used for |
|---|---|---|---|
| `purchases.csv` | one purchase order line | _fill in after your run_ | purchase quantity and dollars |
| `purchase_prices.csv` | one row per brand | _fill in_ | list price and bottle volume |
| `sales.csv` | one sales transaction | _fill in_ | units sold, revenue, excise tax |
| `vendor_invoice.csv` | one vendor invoice | _fill in_ | freight cost |

## Join and filter rules

| Step | Rule | Why |
|---|---|---|
| 1 | Keep purchases where `PurchasePrice > 0` | zero-price lines are free goods / data errors and would distort margins |
| 2 | `purchases` INNER JOIN `purchase_prices` ON `Brand` | brands without a list price can't get a margin; they are dropped |
| 3 | Aggregate sales by `VendorNo, Brand` | sales are only needed as totals per vendor+brand |
| 4 | Purchase summary LEFT JOIN sales summary ON `VendorNumber = VendorNo` AND `Brand` | keep brands that were bought but never sold (sales = 0) |
| 5 | LEFT JOIN freight summary ON `VendorNumber` | freight is only known at vendor level |

## Column mapping

| Target column | Source | Transformation |
|---|---|---|
| `VendorNumber` | `purchases.VendorNumber` | cast to integer; group key |
| `VendorName` | `purchases.VendorName` | trim whitespace |
| `Brand` | `purchases.Brand` | cast to integer; group key |
| `Description` | `purchases.Description` | trim whitespace |
| `PurchasePrice` | `purchases.PurchasePrice` | cast to decimal; group key |
| `ActualPrice` | `purchase_prices.Price` | renamed; list (retail) price |
| `Volume` | `purchase_prices.Volume` | cast to number; non-numeric values (e.g. "Unknown") → 0 |
| `TotalPurchaseQuantity` | `purchases.Quantity` | SUM per vendor+brand |
| `TotalPurchaseDollars` | `purchases.Dollars` | SUM per vendor+brand |
| `TotalSalesQuantity` | `sales.SalesQuantity` | SUM per vendor+brand; 0 if never sold |
| `TotalSalesDollars` | `sales.SalesDollars` | SUM per vendor+brand; 0 if never sold |
| `TotalSalesPrice` | `sales.SalesPrice` | SUM per vendor+brand; 0 if never sold |
| `TotalExciseTax` | `sales.ExciseTax` | SUM per vendor+brand; 0 if never sold |
| `FreightCost` | `vendor_invoice.Freight` | SUM per vendor (repeated on each of the vendor's brands) |
| `GrossProfit` | derived | `TotalSalesDollars − TotalPurchaseDollars` |
| `ProfitMargin` | derived | `GrossProfit / TotalSalesDollars × 100`; 0 when no sales |
| `StockTurnover` | derived | `TotalSalesQuantity / TotalPurchaseQuantity`; 0 when divisor is 0 |
| `SalesToPurchaseRatio` | derived | `TotalSalesDollars / TotalPurchaseDollars`; 0 when divisor is 0 |

## Data-quality checks (run by `spark/vendor_summary_spark.py`)

| Check | Rule | Result is saved in |
|---|---|---|
| Row counts | count every raw input | `run_metrics.json → raw_checks` |
| Null keys | `Brand` / `VendorNumber` must not be null | `raw_checks.*.null_keys` |
| Bad prices | count `PurchasePrice <= 0` rows (filtered out) | `raw_checks.purchases` |
| Negative sales | count `SalesQuantity < 0` rows | `raw_checks.sales` |
| Reconciliation | SUM(`TotalPurchaseDollars`) in target = SUM(`Dollars`) of qualifying raw purchases (to the cent); the job fails if not | `run_metrics.json → reconciliation` |
| Parity with pandas | Spark output vs pandas output: same rows, same keys, same column totals | `spark/compare_with_pandas.py` |

## Known caveat

`FreightCost` is a vendor-level total repeated on every brand row for that vendor, so it must
not be summed across brands (that would multiply-count freight). Use `MAX` or join at vendor level.
