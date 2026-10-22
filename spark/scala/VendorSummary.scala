// VendorSummary.scala
// -------------------
// Scala version of the vendor summary job (same logic as vendor_summary_spark.py),
// written with the Spark DataFrame API. Runs in spark-shell; no build tool needed.
//
//   bash spark/scala/run_scala.sh spark/sample_data spark/output_scala
//
// (The script is fed to spark-shell in :paste mode so the multi-line chained
//  DataFrame expressions compile as one block.)

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

val inputDir  = sys.env.getOrElse("INPUT_DIR", "spark/sample_data")
val outputDir = sys.env.getOrElse("OUTPUT_DIR", "spark/output_scala")
spark.sparkContext.setLogLevel("ERROR")

// Tolerant cast: "Unknown" -> null instead of failing (Spark 4 runs in ANSI mode)
def num(c: String, t: String): Column = expr(s"try_cast(`$c` as $t)").as(c)
def readCsv(name: String): DataFrame = spark.read.option("header", "true").csv(s"$inputDir/$name")

val purchases = readCsv("purchases.csv").select(
  num("VendorNumber", "int"), col("VendorName"), num("Brand", "int"), col("Description"),
  num("PurchasePrice", "double"), num("Quantity", "long"), num("Dollars", "double"))
val prices = readCsv("purchase_prices.csv").select(
  num("Brand", "int"), num("Price", "double").as("ActualPrice"), num("Volume", "double"))
val sales = readCsv("sales.csv").select(
  num("VendorNo", "int"), num("Brand", "int"), num("SalesQuantity", "long"),
  num("SalesDollars", "double"), num("SalesPrice", "double"), num("ExciseTax", "double"))
val invoices = readCsv("vendor_invoice.csv").select(num("VendorNumber", "int"), num("Freight", "double"))

val freight = invoices.groupBy("VendorNumber").agg(sum("Freight").as("FreightCost"))
val purchaseSummary = purchases.filter(col("PurchasePrice") > 0)
  .join(prices, Seq("Brand"), "inner")
  .groupBy("VendorNumber", "VendorName", "Brand", "Description", "PurchasePrice", "ActualPrice", "Volume")
  .agg(sum("Quantity").as("TotalPurchaseQuantity"), sum("Dollars").as("TotalPurchaseDollars"))
val salesSummary = sales.groupBy("VendorNo", "Brand").agg(
  sum("SalesQuantity").as("TotalSalesQuantity"), sum("SalesDollars").as("TotalSalesDollars"),
  sum("SalesPrice").as("TotalSalesPrice"), sum("ExciseTax").as("TotalExciseTax"))

def safeDiv(n: String, d: String): Column = when(col(d) =!= 0, col(n) / col(d)).otherwise(lit(0.0))

val summary = purchaseSummary.as("ps")
  .join(salesSummary.as("ss"), col("ps.VendorNumber") === col("ss.VendorNo") && col("ps.Brand") === col("ss.Brand"), "left")
  .join(freight.as("fs"), col("ps.VendorNumber") === col("fs.VendorNumber"), "left")
  .select(col("ps.*"), col("ss.TotalSalesQuantity"), col("ss.TotalSalesDollars"),
          col("ss.TotalSalesPrice"), col("ss.TotalExciseTax"), col("fs.FreightCost"))
  .na.fill(0, Seq("Volume", "TotalSalesQuantity", "TotalSalesDollars", "TotalSalesPrice", "TotalExciseTax", "FreightCost"))
  .withColumn("VendorName", trim(col("VendorName")))
  .withColumn("Description", trim(col("Description")))
  .withColumn("GrossProfit", col("TotalSalesDollars") - col("TotalPurchaseDollars"))
  .withColumn("ProfitMargin", when(col("TotalSalesDollars") =!= 0,
                                   col("GrossProfit") / col("TotalSalesDollars") * 100).otherwise(lit(0.0)))
  .withColumn("StockTurnover", safeDiv("TotalSalesQuantity", "TotalPurchaseQuantity"))
  .withColumn("SalesToPurchaseRatio", safeDiv("TotalSalesDollars", "TotalPurchaseDollars"))
  .orderBy(col("TotalPurchaseDollars").desc)
  .cache()

// Reconciliation: summary purchase dollars must equal qualifying raw purchases
val expected = purchases.filter(col("PurchasePrice") > 0).join(prices.select("Brand"), Seq("Brand"))
  .agg(sum("Dollars")).first().getDouble(0)
val actual = summary.agg(sum("TotalPurchaseDollars")).first().getDouble(0)
val matched = math.abs(expected - actual) < 0.01

println(s"Summary rows: ${summary.count()}")
println(f"Reconciliation: expected=$expected%.2f actual=$actual%.2f match=$matched")
summary.select("VendorName", "Brand", "TotalPurchaseDollars", "GrossProfit", "ProfitMargin").show(5, truncate = false)
summary.write.mode("overwrite").parquet(s"$outputDir/vendor_sales_summary_parquet")
println(s"Wrote Parquet to $outputDir/vendor_sales_summary_parquet")
System.exit(if (matched) 0 else 1)
