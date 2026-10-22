"""
make_sample_data.py
-------------------
Generates a SMALL synthetic copy of the four raw files the pipeline needs, using the
same column names as the real dataset. Used by the tests and by CI (GitHub can't
store the real multi-GB CSVs).

    python spark/make_sample_data.py --out spark/sample_data
"""
import argparse
import csv
import os
import random

VENDORS = [(1128, "BROWN-FORMAN CORP"), (4425, "MARTIGNETTI COMPANIES"),
           (3960, "DIAGEO NORTH AMERICA INC"), (12546, "JIM BEAM BRANDS COMPANY"),
           (9552, "M S WALKER INC")]


def main(out_dir, n_purchases=2000, n_sales=3000, seed=42):
    random.seed(seed)
    os.makedirs(out_dir, exist_ok=True)

    # 40 brands, each owned by one vendor, with a list price and bottle volume
    brands = []
    for b in range(1, 41):
        vnum, vname = VENDORS[b % len(VENDORS)]
        cost = round(random.uniform(5, 40), 2)
        brands.append({"Brand": 1000 + b, "VendorNumber": vnum, "VendorName": vname,
                       "Description": f"Sample Product {b}", "PurchasePrice": cost,
                       "Price": round(cost * random.uniform(1.2, 1.8), 2),
                       "Volume": random.choice(["750", "1000", "1750", "Unknown"])})

    with open(os.path.join(out_dir, "purchase_prices.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["Brand", "Description", "Price", "Size", "Volume", "Classification",
                    "PurchasePrice", "VendorNumber", "VendorName"])
        for br in brands[:-2]:  # last 2 brands have no price row -> dropped by the inner join
            w.writerow([br["Brand"], br["Description"], br["Price"], "750mL", br["Volume"], 1,
                        br["PurchasePrice"], br["VendorNumber"], br["VendorName"]])

    with open(os.path.join(out_dir, "purchases.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["InventoryId", "Store", "Brand", "Description", "Size", "VendorNumber",
                    "VendorName", "PONumber", "PODate", "ReceivingDate", "InvoiceDate",
                    "PayDate", "PurchasePrice", "Quantity", "Dollars", "Classification"])
        for i in range(n_purchases):
            br = random.choice(brands)
            qty = random.randint(1, 60)
            price = 0.0 if i % 250 == 0 else br["PurchasePrice"]  # a few zero-price rows (filtered out)
            w.writerow([f"1_X_{br['Brand']}", 1, br["Brand"], f"  {br['Description']}  ", "750mL",
                        br["VendorNumber"], f"{br['VendorName']}  ", 8000 + i, "2024-01-02",
                        "2024-01-09", "2024-01-10", "2024-02-10", price, qty,
                        round(price * qty, 2), 1])

    with open(os.path.join(out_dir, "sales.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["InventoryId", "Store", "Brand", "Description", "Size", "SalesQuantity",
                    "SalesDollars", "SalesPrice", "SalesDate", "Volume", "Classification",
                    "ExciseTax", "VendorNo", "VendorName"])
        for i in range(n_sales):
            br = random.choice(brands[:-5])  # last 5 brands never sell
            qty = random.randint(1, 12)
            w.writerow([f"1_X_{br['Brand']}", 1, br["Brand"], br["Description"], "750mL", qty,
                        round(br["Price"] * qty, 2), br["Price"], f"2024-01-{1 + i % 28:02d}",
                        br["Volume"], 1, round(0.79 * qty, 2), br["VendorNumber"],
                        br["VendorName"]])

    with open(os.path.join(out_dir, "vendor_invoice.csv"), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["VendorNumber", "VendorName", "InvoiceDate", "PONumber", "PODate",
                    "PayDate", "Quantity", "Dollars", "Freight", "Approval"])
        for i, (vnum, vname) in enumerate(VENDORS * 3):
            w.writerow([vnum, vname, "2024-01-10", 9000 + i, "2024-01-02", "2024-02-10",
                        100, 1000.0, round(random.uniform(10, 300), 2), "None"])

    print(f"Sample data written to {out_dir}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=os.path.join(os.path.dirname(__file__), "sample_data"))
    main(ap.parse_args().out)
