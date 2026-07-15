# Vendor Performance Analysis — End-to-End Data Analytics Project

An end-to-end data analytics case study analysing a full year of purchasing and sales data
across **126 vendors** and **10,692 vendor–brand combinations** to improve profitability and
inventory efficiency. Built with **SQL, Python, and Power BI**.

---

## 📌 Business Problem

Effective inventory and sales management are critical to profitability in retail & wholesale.
This project set out to:

1. Identify underperforming brands needing promotional or pricing adjustments.
2. Determine the top vendors contributing to sales and gross profit.
3. Analyse the impact of bulk purchasing on unit costs.
4. Assess inventory turnover to reduce holding costs.
5. Investigate the profitability variance between high- and low-performing vendors.

---

## 🔧 Tech Stack & Workflow

| Stage | Tools | What it does |
|-------|-------|--------------|
| **Ingestion** | Python, SQLite | Load 6 raw CSVs (15M+ rows) into a database (chunked for large files) |
| **Aggregation** | SQL (CTEs, joins) | Merge tables into one clean `vendor_sales_summary` table with business metrics |
| **EDA & Analysis** | pandas, matplotlib, seaborn | Explore, clean, and answer the 5 business questions |
| **Statistics** | SciPy | Hypothesis testing (Welch's t-test) + confidence intervals |
| **Dashboard** | Power BI | Interactive dashboard (KPIs, DAX measures) |
| **Report** | reportlab | Formal PDF business report |

```
Raw CSVs ──> SQLite DB ──> Aggregated Table ──> Python EDA + Stats ──> Power BI ──> Report
```

---

## 📁 Repository Structure

| File | Description |
|------|-------------|
| `ingestion_db.py` | Loads the raw CSVs into `inventory.db` (SQLite) |
| `get_vendor_summary.py` | Builds the cleaned, aggregated `vendor_sales_summary` table |
| `Vendor Performance Analysis.ipynb` | EDA, the 5 business questions, and the hypothesis test |
| `generate_report.py` | Generates the formal PDF report |
| `vendor_sales_summary.csv` | Final aggregated dataset (powers the dashboard) |
| `Vendor_Performance_Report.pdf` | The final written report |

> **Note:** The raw data files (`sales.csv`, `purchases.csv`, etc.) and `inventory.db`
> are excluded from the repo due to size (several GB). Run the scripts below to regenerate them.

---

## ▶️ How to Reproduce

```bash
# 1. Install dependencies
pip install pandas numpy sqlalchemy matplotlib seaborn scipy reportlab

# 2. Place the raw CSVs in this folder, then load them into the database
python ingestion_db.py

# 3. Build the aggregated summary table
python get_vendor_summary.py

# 4. Open the analysis notebook
jupyter notebook "Vendor Performance Analysis.ipynb"

# 5. (Optional) Generate the PDF report
python generate_report.py
```

---

## 📊 Key Findings

- **Underperforming brands:** 198 brands have high margins (≥65%) but very low sales (≤$560) —
  prime promotion targets.
- **Vendor dependency:** the top 10 of 126 vendors drive **65.7%** of purchase spend (Diageo alone 16.3%).
- **Bulk purchasing:** large orders cost **35–40% less per unit** than small orders.
- **Dead capital:** **$15.6M** is locked in unsold inventory.
- **Margins (statistically validated):** low-volume vendors earn significantly higher margins
  (41.6% vs 31.2%, Welch's t-test *p* < 0.001) — a premium/niche vs. high-volume/low-margin split.

---

## 💡 Recommendations

1. Promote high-margin, low-sales brands to grow volume with minimal margin risk.
2. Mitigate vendor-dependency risk with contracts and backup suppliers for top vendors.
3. Consolidate small orders to capture bulk discounts.
4. Release the $15.6M of frozen capital by aligning purchases to sales velocity.
5. Differentiate vendor strategy — high-volume for total profit, low-volume for margin.
