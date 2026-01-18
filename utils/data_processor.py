
import os
import csv
import argparse
from typing import List, Dict, Tuple
from collections import defaultdict
from pathlib import Path

import matplotlib
matplotlib.use("Agg")  # Non-interactive backend to save figures
import matplotlib.pyplot as plt

# ======= DEFAULT: Data folder next to this script =======
base_dir = Path(__file__).parent.parent
DATA_DIR = base_dir / "output"
DATA_DIR.mkdir(exist_ok=True)
DEFAULT_FILE_PATH = DATA_DIR / "Sales_cleaned_data.txt"
# ========================================================

# ----------------------------------------------------------------------
# Loader: read Sales_cleaned_data.txt (pipe-delimited)
# ----------------------------------------------------------------------
def load_cleaned_transactions(filepath: Path) -> List[Dict]:
    encodings = ['utf-8', 'latin-1', 'cp1252']
    rows = None
    last_err = None

    # Ensure filepath is a Path
    filepath = Path(filepath)

    if not filepath.exists():
        raise FileNotFoundError(
            f"File not found: {filepath}\n"
            f"Script base directory: {base_dir}\n"
            f"Current working directory: {Path.cwd()}"
        )

    for enc in encodings:
        try:
            with filepath.open('r', encoding=enc, newline='') as f:
                reader = csv.reader(f, delimiter='|')
                rows = list(reader)
            break
        except UnicodeDecodeError as e:
            last_err = e
            rows = None

    if rows is None:
        raise UnicodeDecodeError(
            "read", b"", 0, 1,
            f"Unable to decode file with encodings {encodings}. Last error: {last_err}"
        )

    if not rows:
        return []

    # Skip header row if present
    header = rows[0]
    data_rows = rows[1:] if header and "TransactionID" in header else rows

    txs: List[Dict] = []
    for r in data_rows:
        if not r or len(r) < 8:
            continue

        # Pad to 9 columns if Amount missing
        while len(r) < 9:
            r.append('')

        tx_id, date, prod_id, prod_name, qty_str, price_str, cust_id, region, amount_str = r[:9]

        # Parse types (robust)
        qty = None
        unit_price = None
        amount = None

        try:
            qty = int(qty_str)
        except ValueError:
            try:
                qty = int(float(qty_str))  # handle "10.0"
            except Exception:
                qty = None

        try:
            unit_price = float(price_str)
        except ValueError:
            unit_price = None

        try:
            amount = float(amount_str) if amount_str not in (None, '') else None
        except ValueError:
            amount = None

        # If Amount missing but Quantity & UnitPrice present, compute it
        if amount is None and qty is not None and unit_price is not None:
            amount = qty * unit_price

        txs.append({
            'TransactionID': tx_id.strip(),
            'Date': date.strip(),
            'ProductID': prod_id.strip(),
            'ProductName': prod_name.strip(),
            'Quantity': qty,
            'UnitPrice': unit_price,
            'CustomerID': (cust_id or '').strip(),
            'Region': (region or '').strip(),
            'Amount': amount,
        })

    return txs

# ----------------------------------------------------------------------
# Task 1 - Calculate total revenue
# ----------------------------------------------------------------------
def calculate_total_revenue(transactions: List[Dict]) -> float:

    total = 0.0
    for t in transactions:
        qty = t.get('Quantity')
        price = t.get('UnitPrice')
        if isinstance(qty, int) and isinstance(price, (int, float)):
            total += qty * float(price)
    return float(total)

# ----------------------------------------------------------------------
# Task 2 - Region wise Sales analysis
# ----------------------------------------------------------------------
def region_wise_sales(transactions: List[Dict]) -> Dict[str, Dict[str, float]]:

    agg = {}
    grand_total = 0.0

    for t in transactions:
        region = (t.get('Region') or '').strip()
        qty = t.get('Quantity')
        price = t.get('UnitPrice')
        if not isinstance(qty, int) or not isinstance(price, (int, float)):
            continue

        amt = qty * float(price)
        grand_total += amt

        if region not in agg:
            agg[region] = {'total_sales': 0.0, 'transaction_count': 0}
        agg[region]['total_sales'] += amt
        agg[region]['transaction_count'] += 1

    sorted_regions = sorted(agg.items(), key=lambda kv: kv[1]['total_sales'], reverse=True)
    result = {}
    for region, stats in sorted_regions:
        pct = (stats['total_sales'] / grand_total * 100.0) if grand_total > 0 else 0.0
        result[region] = {
            'total_sales': round(stats['total_sales'], 2),
            'transaction_count': stats['transaction_count'],
            'percentage': round(pct, 2)
        }
    return result

# ----------------------------------------------------------------------
# Task 3 - Top selling products
# ----------------------------------------------------------------------
def top_selling_products(transactions: List[Dict], n: int = 5) -> List[Tuple[str, int, float]]:

    by_product = {}
    for t in transactions:
        pname = t.get('ProductName', '')
        qty = t.get('Quantity')
        price = t.get('UnitPrice')
        if pname == '' or not isinstance(qty, int) or not isinstance(price, (int, float)):
            continue

        amt = qty * float(price)
        if pname not in by_product:
            by_product[pname] = {'q': 0, 'rev': 0.0}
        by_product[pname]['q'] += qty
        by_product[pname]['rev'] += amt

    sorted_items = sorted(by_product.items(), key=lambda kv: kv[1]['q'], reverse=True)
    top = [(name, stats['q'], round(stats['rev'], 2)) for name, stats in sorted_items[:n]]
    return top

# ----------------------------------------------------------------------
# Task 4 - Customer Purchase analysis
# ----------------------------------------------------------------------
def customer_analysis(transactions: List[Dict]) -> Dict[str, Dict]:

    agg = {}
    for t in transactions:
        cid = t.get('CustomerID', '')
        qty = t.get('Quantity')
        price = t.get('UnitPrice')
        pname = t.get('ProductName', '')
        if cid == '' or not isinstance(qty, int) or not isinstance(price, (int, float)):
            continue

        amt = qty * float(price)
        if cid not in agg:
            agg[cid] = {'total_spent': 0.0, 'purchase_count': 0, 'products': set()}
        agg[cid]['total_spent'] += amt
        agg[cid]['purchase_count'] += 1
        if pname:
            agg[cid]['products'].add(pname)

    sorted_customers = sorted(agg.items(), key=lambda kv: kv[1]['total_spent'], reverse=True)
    result = {}
    for cid, stats in sorted_customers:
        avg = stats['total_spent'] / stats['purchase_count'] if stats['purchase_count'] > 0 else 0.0
        result[cid] = {
            'total_spent': round(stats['total_spent'], 2),
            'purchase_count': stats['purchase_count'],
            'avg_order_value': round(avg, 2),
            'products_bought': sorted(stats['products'])
        }
    return result

# ----------------------------------------------------------------------
# Task 5 - Daily Sales Trend
# ----------------------------------------------------------------------
def daily_sales_trend(transactions: List[Dict]) -> Dict[str, Dict]:
    """
    Returns: {date: {'revenue','transaction_count','unique_customers'}} sorted by date
    """
    agg = {}
    for t in transactions:
        dt = t.get('Date', '')
        cid = t.get('CustomerID', '')
        qty = t.get('Quantity')
        price = t.get('UnitPrice')
        if dt == '' or not isinstance(qty, int) or not isinstance(price, (int, float)):
            continue

        amt = qty * float(price)
        if dt not in agg:
            agg[dt] = {'revenue': 0.0, 'transaction_count': 0, 'customers': set()}
        agg[dt]['revenue'] += amt
        agg[dt]['transaction_count'] += 1
        if cid:
            agg[dt]['customers'].add(cid)

    ordered_dates = sorted(agg.keys())  # ISO dates sort chronologically
    result = {}
    for dt in ordered_dates:
        result[dt] = {
            'revenue': round(agg[dt]['revenue'], 2),
            'transaction_count': agg[dt]['transaction_count'],
            'unique_customers': len(agg[dt]['customers'])
        }
    return result

# ----------------------------------------------------------------------
# Task 6 - Find Peak Sales Day
# ----------------------------------------------------------------------
def find_peak_sales_day(transactions: List[Dict]) -> Tuple[str, float, int]:
    """
    Returns: (date, revenue, transaction_count)
    """
    trend = daily_sales_trend(transactions)
    if not trend:
        return ('', 0.0, 0)
    max_rev = max(v['revenue'] for v in trend.values())
    candidates = [d for d, v in trend.items() if v['revenue'] == max_rev]
    best_date = sorted(candidates)[0]
    return (best_date, trend[best_date]['revenue'], trend[best_date]['transaction_count'])

# ----------------------------------------------------------------------
# Task 7 - Low Performing products
# ----------------------------------------------------------------------
def low_performing_products(transactions: List[Dict], threshold: int = 10) -> List[Tuple[str, int, float]]:
    """
    Returns: list of tuples (ProductName, TotalQuantity, TotalRevenue) sorted by TotalQuantity asc
    """
    by_product = {}
    for t in transactions:
        pname = t.get('ProductName', '')
        qty = t.get('Quantity')
        price = t.get('UnitPrice')
        if pname == '' or not isinstance(qty, int) or not isinstance(price, (int, float)):
            continue

        amt = qty * float(price)
        if pname not in by_product:
            by_product[pname] = {'q': 0, 'rev': 0.0}
        by_product[pname]['q'] += qty
        by_product[pname]['rev'] += amt

    low = [(name, stats['q'], round(stats['rev'], 2))
           for name, stats in by_product.items() if stats['q'] < threshold]
    low_sorted = sorted(low, key=lambda x: x[1])  # by quantity asc
    return low_sorted

# ----------------------------------------------------------------------
# Helpers: formatted printing (tables)
# ----------------------------------------------------------------------
def print_money(amount: float) -> str:
    return f"{amount:,.2f}"

def print_region_summary_table(region_stats: Dict[str, Dict]):
    headers = ["Region", "Total Sales", "Txn Count", "% Share"]
    region_col_width = max(len("Region"), *(len(r) if r else 0 for r in region_stats.keys()))
    header_line = f"{headers[0]:<{region_col_width}}  {headers[1]:>15}  {headers[2]:>10}  {headers[3]:>8}"
    sep_line = "-" * len(header_line)
    print(header_line)
    print(sep_line)
    for region, s in region_stats.items():
        name = region if region else "(Unknown)"
        print(
            f"{name:<{region_col_width}}  "
            f"{print_money(s['total_sales']):>15}  "
            f"{s['transaction_count']:>10d}  "
            f"{s['percentage']:>8.2f}"
        )

def print_top_products_table(top_products: List[Tuple[str, int, float]]):
    headers = ["Product", "Total Qty", "Total Revenue"]
    prod_col_width = max(len("Product"), *(len(p[0]) for p in top_products)) if top_products else len("Product")
    header_line = f"{headers[0]:<{prod_col_width}}  {headers[1]:>10}  {headers[2]:>15}"
    sep_line = "-" * len(header_line)
    print(header_line)
    print(sep_line)
    for name, q, rev in top_products:
        print(f"{name:<{prod_col_width}}  {q:>10d}  {print_money(rev):>15}")

def print_customer_summary_table(customer_stats: Dict[str, Dict], top_n: int = 10):
    headers = ["CustomerID", "Total Spent", "Purchases", "Avg Order", "Products"]
    cust_col_width = len("CustomerID")
    header_line = f"{headers[0]:<{cust_col_width}}  {headers[1]:>15}  {headers[2]:>10}  {headers[3]:>12}  {headers[4]}"
    sep_line = "-" * len(header_line)
    print(header_line)
    print(sep_line)
    for cid, s in list(customer_stats.items())[:top_n]:
        products = ", ".join(s['products_bought'])
        print(
            f"{cid:<{cust_col_width}}  "
            f"{print_money(s['total_spent']):>15}  "
            f"{s['purchase_count']:>10d}  "
            f"{print_money(s['avg_order_value']):>12}  "
            f"{products}"
        )

def print_daily_trend_table(trend: Dict[str, Dict], max_rows: int = 15):
    headers = ["Date", "Revenue", "Txn Count", "Unique Cust"]
    header_line = f"{headers[0]:<12}  {headers[1]:>15}  {headers[2]:>10}  {headers[3]:>12}"
    sep_line = "-" * len(header_line)
    print(header_line)
    print(sep_line)
    items = list(trend.items())
    rows = items[:max_rows] if len(items) > max_rows else items
    for dt, s in rows:
        print(
            f"{dt:<12}  "
            f"{print_money(s['revenue']):>15}  "
            f"{s['transaction_count']:>10d}  "
            f"{s['unique_customers']:>12d}"
        )
    if len(items) > max_rows:
        print(f"... ({len(items) - max_rows} more days)")

# ----------------------------------------------------------------------
# Visualizations (charts saved as PNG)
# ----------------------------------------------------------------------
def get_output_dir() -> Path:
    """
    Create analytics_outputs next to the script (or next to the input file),
    depending on how you want to scope outputs. Here we scope to script base.
    """
    base_dir = Path(__file__).parent.parent
    out_dir = base_dir / "output"
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir

def plot_region_sales_bar(region_stats: Dict[str, Dict], out_dir: Path = None) -> Path:
    if out_dir is None:
        out_dir = get_output_dir()
    regions = list(region_stats.keys())
    sales = [region_stats[r]['total_sales'] for r in regions]
    plt.figure(figsize=(10, 6))
    bars = plt.bar(regions, sales, color="#4C78A8")
    plt.title("Region-wise Total Sales")
    plt.xlabel("Region")
    plt.ylabel("Total Sales")
    plt.xticks(rotation=0)
    # annotate bars
    for bar, val in zip(bars, sales):
        plt.text(bar.get_x() + bar.get_width()/2, bar.get_height(), f"{val:,.0f}",
                 ha="center", va="bottom", fontsize=9)
    out_path = out_dir / "region_sales_bar.png"
    plt.tight_layout()
    plt.savefig(out_path.as_posix(), dpi=120)
    plt.close()
    return out_path

def plot_top_products_bar(top_products: List[Tuple[str, int, float]], out_dir: Path) -> Path:
    names = [t[0] for t in top_products]
    qtys = [t[1] for t in top_products]
    plt.figure(figsize=(10, 6))
    bars = plt.bar(names, qtys, color="#F58518")
    plt.title("Top Products by Quantity")
    plt.xlabel("Product")
    plt.ylabel("Total Quantity")
    plt.xticks(rotation=30, ha="right")
    for bar, val in zip(bars, qtys):
        plt.text(bar.get_x() + bar.get_width()/2, bar.get_height(), f"{val}",
                 ha="center", va="bottom", fontsize=9)
    out_path = out_dir / "top_products_bar.png"
    plt.tight_layout()
    plt.savefig(out_path.as_posix(), dpi=120)
    plt.close()
    return out_path

def plot_daily_revenue_line(trend: Dict[str, Dict], out_dir: Path) -> Path:
    dates = list(trend.keys())
    revs = [trend[d]['revenue'] for d in dates]
    plt.figure(figsize=(12, 5))
    plt.plot(dates, revs, marker="o", color="#54A24B")
    plt.title("Daily Revenue Trend")
    plt.xlabel("Date")
    plt.ylabel("Revenue")
    plt.xticks(rotation=45, ha="right")
    plt.grid(True, linestyle="--", alpha=0.4)
    out_path = out_dir / "daily_revenue_trend.png"
    plt.tight_layout()
    plt.savefig(out_path.as_posix(), dpi=120)
    plt.close()
    return out_path

# ----------------------------------------------------------------------
# Optional CSV exports
# ----------------------------------------------------------------------
def export_region_csv(region_stats: Dict[str, Dict], out_dir: Path) -> Path:
    path = out_dir / "region_sales_summary.csv"
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["Region", "Total Sales", "Transaction Count", "Percentage"])
        for region, s in region_stats.items():
            w.writerow([region, s["total_sales"], s["transaction_count"], s["percentage"]])
    return path

def export_top_products_csv(top_products: List[Tuple[str, int, float]], out_dir: Path) -> Path:
    path = out_dir / "top_products_summary.csv"
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["ProductName", "TotalQuantity", "TotalRevenue"])
        for name, q, rev in top_products:
            w.writerow([name, q, rev])
    return path

def export_customers_csv(customer_stats: Dict[str, Dict], out_dir: Path) -> Path:
    path = out_dir / "customer_summary.csv"
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["CustomerID", "TotalSpent", "PurchaseCount", "AvgOrderValue", "ProductsBought"])
        for cid, s in customer_stats.items():
            w.writerow([cid, s["total_spent"], s["purchase_count"], s["avg_order_value"], ", ".join(s["products_bought"])])
    return path

def export_daily_trend_csv(trend: Dict[str, Dict], out_dir: Path) -> Path:
    path = out_dir / "daily_trend_summary.csv"
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["Date", "Revenue", "TransactionCount", "UniqueCustomers"])
        for dt, s in trend.items():
            w.writerow([dt, s["revenue"], s["transaction_count"], s["unique_customers"]])
    return path

def export_low_products_csv(low_products: List[Tuple[str, int, float]], out_dir: Path) -> Path:
    path = out_dir / "low_performing_products.csv"
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["ProductName", "TotalQuantity", "TotalRevenue"])
        for name, q, rev in low_products:
            w.writerow([name, q, rev])
    return path

# ----------------------------------------------------------------------
# CLI / Main
# ----------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Sales analytics with visual and formatted output from Sales_cleaned_data.txt"
    )
    parser.add_argument(
        "--file", "-f",
        help="Path to Sales_cleaned_data.txt (optional; defaults to ./output/Sales_cleaned_data.txt next to script)",
        default=None
    )
    parser.add_argument("--top-n", "-n", type=int, default=5, help="Top N products by quantity (default: 5)")
    parser.add_argument("--low-threshold", "-t", type=int, default=10, help="Low-performing quantity threshold (default: 10)")
    parser.add_argument("--export", action="store_true", help="Export CSV summaries and save chart images")
    parser.add_argument("--print-all-days", action="store_true", help="Print the full daily trend table")
    args = parser.parse_args()

    # If user provided a file, use it (relative paths allowed); otherwise default next to script
    infile = Path(args.file).resolve() if args.file else DEFAULT_FILE_PATH

    if not Path(infile).exists():
        raise FileNotFoundError(
            f"File not found: {infile}\n"
            f"Expected default: {DEFAULT_FILE_PATH}\n"
            f"Script base directory: {base_dir}\n"
            f"Current working directory: {Path.cwd()}"
        )

    transactions = load_cleaned_transactions(Path(infile))
    out_dir = get_output_dir() if args.export else None

    # Task 1: Total revenue
    total_rev = calculate_total_revenue(transactions)
    print("\n=== Task 1: Total Revenue ===")
    print(f"Total Revenue: {print_money(total_rev)}")

    # Task 2: Region-wise sales
    region_stats = region_wise_sales(transactions)
    print("\n=== Task 2: Region-wise Sales (sorted by total_sales desc) ===")
    print_region_summary_table(region_stats)
    if args.export and out_dir:
        img1 = plot_region_sales_bar(region_stats, out_dir)
        csv1 = export_region_csv(region_stats, out_dir)
        print(f"(Saved chart: {img1})")
        print(f"(Exported CSV: {csv1})")

    # Task 3: Top selling products
    top_products = top_selling_products(transactions, n=args.top_n)
    print(f"\n=== Task 3: Top {args.top_n} Products by Quantity ===")
    print_top_products_table(top_products)
    if args.export and out_dir:
        img2 = plot_top_products_bar(top_products, out_dir)
        csv2 = export_top_products_csv(top_products, out_dir)
        print(f"(Saved chart: {img2})")
        print(f"(Exported CSV: {csv2})")

    # Task 4: Customer analysis
    customer_stats = customer_analysis(transactions)
    print("\n=== Task 4: Customer Purchase Analysis (Top 10 by Total Spent) ===")
    print_customer_summary_table(customer_stats, top_n=10)
    if args.export and out_dir:
        csv3 = export_customers_csv(customer_stats, out_dir)
        print(f"(Exported CSV: {csv3})")

    # Task 5: Daily sales trend
    trend = daily_sales_trend(transactions)
    print("\n=== Task 5: Daily Sales Trend ===")
    print_daily_trend_table(trend, max_rows=(10_000 if args.print_all_days else 15))
    if args.export and out_dir:
        img3 = plot_daily_revenue_line(trend, out_dir)
        csv4 = export_daily_trend_csv(trend, out_dir)
        print(f"(Saved chart: {img3})")
        print(f"(Exported CSV: {csv4})")

    # Task 6: Peak sales day
    peak_day = find_peak_sales_day(transactions)
    print("\n=== Task 6: Peak Sales Day ===")
    print(f"{peak_day[0]} | Revenue: {print_money(peak_day[1])} | Transactions: {peak_day[2]}")

    # Task 7: Low performing products
    low_products = low_performing_products(transactions, threshold=args.low_threshold)
    print(f"\n=== Task 7: Low Performing Products (qty < {args.low_threshold}) ===")
    if low_products:
        print_top_products_table([(n, q, r) for n, q, r in low_products])
    else:
        print("No products below the specified threshold.")
    if args.export and out_dir:
        csv5 = export_low_products_csv(low_products, out_dir)
        print(f"(Exported CSV: {csv5})")

    print("\nDone.")

if __name__ == "__main__":
    main()
