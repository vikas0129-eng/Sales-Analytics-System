import os
import sys
from typing import List, Dict, Tuple
from pathlib import Path

# Set up project root and Data directory from utils folder
base = Path(__file__).parent.parent  # project root from utils/
data_dir = base / "data"
output_dir = base / "output"
data_dir.mkdir(exist_ok=True)
output_dir.mkdir(parents=True, exist_ok=True)

HEADERS = {"TransactionID", "Date", "ProductID", "ProductName",
           "Quantity", "UnitPrice", "CustomerID", "Region"}

# Task 1 - Read and clean the file
def read_sales_data(filename: Path) -> List[str]:
    encodings = ['utf-8', 'latin-1', 'cp1252']
    text = None
    last_err = None

    if not filename.exists():
        raise FileNotFoundError(f"File not found: {filename}")

    for enc in encodings:
        try:
            with filename.open('r', encoding=enc) as f:
                text = f.read()
            break
        except UnicodeDecodeError as e:
            last_err = e
            continue

    if text is None:
        raise UnicodeDecodeError("read", b"", 0, 1,
                                 f"Unable to decode file with encodings: {encodings}. Last error: {last_err}")

    # Normalize newlines and strip
    lines = [ln.strip() for ln in text.splitlines()]
    # Remove empty lines
    lines = [ln for ln in lines if ln]
    # Skip header tokens wherever they appear as stand-alone lines
    lines_wo_headers = [ln for ln in lines if ln not in HEADERS]

    # If file is already pipe-delimited
    pipe_count = sum(1 for ln in lines_wo_headers if '|' in ln)
    if pipe_count >= max(1, len(lines_wo_headers) // 2):
        raw_records = []
        for ln in lines_wo_headers:
            if '|' in ln:
                parts = ln.split('|')
                if len(parts) == 8:
                    raw_records.append(ln)
                else:
                    # defensive repair in case extra pipes appear
                    if len(parts) > 8:
                        repaired = [
                            parts[0], parts[1], parts[2],
                            '|'.join(parts[3:-4]),
                            parts[-4], parts[-3], parts[-2], parts[-1]
                        ]
                        raw_records.append('|'.join(repaired))
        return raw_records

    # Otherwise treat as vertical records: 8 fields per record starting at TransactionID
    raw_records = []
    i = 0
    n = len(lines_wo_headers)
    while i < n:
        token = lines_wo_headers[i]
        if token and token[0] in {'T', 'X'}:  # TransactionID start
            record_tokens = [token]
            j = i + 1
            while j < n and len(record_tokens) < 8:
                record_tokens.append(lines_wo_headers[j])
                j += 1
            i = j
            if len(record_tokens) == 8:
                raw_records.append('|'.join(record_tokens))
            # else: skip incomplete records silently
        else:
            i += 1

    return raw_records

# Task 2 - Parse and Clean the data
def parse_transactions(raw_lines: List[str]) -> List[Dict]:
    transactions = []
    for ln in raw_lines:
        parts = ln.split('|')
        if len(parts) != 8:
            continue

        tx_id, date, prod_id, prod_name, qty_str, price_str, cust_id, region = parts

        # Replace commas in product name, remove commas from numerics
        prod_name_clean = prod_name.replace(',', ' ').strip()
        qty_clean = qty_str.replace(',', '').strip()
        price_clean = price_str.replace(',', '').strip()

        try:
            qty_val = int(qty_clean)
        except ValueError:
            continue

        try:
            price_val = float(price_clean)
        except ValueError:
            continue

        transactions.append({
            'TransactionID': tx_id.strip(),
            'Date': date.strip(),
            'ProductID': prod_id.strip(),
            'ProductName': prod_name_clean,
            'Quantity': qty_val,        # int
            'UnitPrice': price_val,     # float
            'CustomerID': cust_id.strip(),
            'Region': region.strip(),
        })
    return transactions

# Task 3  - Validation and Filter
def validate_and_filter(
    transactions: List[Dict],
    region: str = None,
    min_amount: float = None,
    max_amount: float = None
) -> Tuple[List[Dict], int, Dict]:

    required_fields = ['TransactionID', 'Date', 'ProductID', 'ProductName',
                       'Quantity', 'UnitPrice', 'CustomerID', 'Region']

    valid = []
    invalid_count = 0

    for t in transactions:
        if any((k not in t or t[k] in [None, '']) for k in required_fields):
            invalid_count += 1
            continue
        if not (isinstance(t['Quantity'], int) and t['Quantity'] > 0):
            invalid_count += 1
            continue
        if not (isinstance(t['UnitPrice'], (int, float)) and t['UnitPrice'] > 0):
            invalid_count += 1
            continue
        if not (isinstance(t['TransactionID'], str) and t['TransactionID'].startswith('T')):
            invalid_count += 1
            continue
        if not (isinstance(t['ProductID'], str) and t['ProductID'].startswith('P')):
            invalid_count += 1
            continue
        if not (isinstance(t['CustomerID'], str) and t['CustomerID'].startswith('C')):
            invalid_count += 1
            continue

        t['Amount'] = float(t['Quantity'] * t['UnitPrice'])
        valid.append(t)

    total_input = len(transactions)

    # Display options
    regions = sorted({t['Region'] for t in valid if t['Region']})
    print(f"Available regions: {', '.join(regions) if regions else '(none)'}")

    if valid:
        amounts = [t['Amount'] for t in valid]
        min_amt, max_amt = min(amounts), max(amounts)
        print(f"Transaction amount range (min/max): {min_amt:.2f} / {max_amt:.2f}")
    else:
        print("No valid transactions to compute amount range.")
        min_amt = max_amt = None

    # Apply filters
    filtered = valid
    filtered_by_region = 0
    filtered_by_amount = 0

    if region:
        before = len(filtered)
        filtered = [t for t in filtered if t['Region'].lower() == region.lower()]
        filtered_by_region = before - len(filtered)
        print(f"After region filter ('{region}'): {len(filtered)} records (removed {filtered_by_region})")

    if min_amount is not None:
        before = len(filtered)
        filtered = [t for t in filtered if t['Amount'] >= float(min_amount)]
        removed = before - len(filtered)
        filtered_by_amount += removed
        print(f"After min_amount filter ({min_amount}): {len(filtered)} records (removed {removed})")

    if max_amount is not None:
        before = len(filtered)
        filtered = [t for t in filtered if t['Amount'] <= float(max_amount)]
        removed = before - len(filtered)
        filtered_by_amount += removed
        print(f"After max_amount filter ({max_amount}): {len(filtered)} records (removed {removed})")

    summary = {
        'total_input': total_input,
        'invalid': invalid_count,
        'filtered_by_region': filtered_by_region,
        'filtered_by_amount': filtered_by_amount,
        'final_count': len(filtered)
    }

    return filtered, invalid_count, summary

# Save cleaned data back to Output folder
def save_cleaned_data(transactions: List[Dict], output_dir: Path,
                      out_name: str = 'Sales_Cleaned_data.txt') -> Path:
    """Save cleaned transactions as pipe-delimited text into output_dir (Data)."""
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / out_name
    header = 'TransactionID|Date|ProductID|ProductName|Quantity|UnitPrice|CustomerID|Region|Amount'
    with out_path.open('w', encoding='utf-8') as f:
        f.write(header + '\n')
        for t in transactions:
            row = [
                t['TransactionID'],
                t['Date'],
                t['ProductID'],
                t['ProductName'],
                str(t['Quantity']),
                f"{t['UnitPrice']:.2f}",
                t['CustomerID'],
                t['Region'],
                f"{t['Amount']:.2f}"
            ]
            f.write('|'.join(row) + '\n')
    return out_path

if __name__ == '__main__':
    # Input file in data/
    infile = data_dir / 'sales_data.txt'

    try:
        raw = read_sales_data(infile)
    except FileNotFoundError as e:
        print(str(e))
        sys.exit(1)
    except UnicodeDecodeError as e:
        print(f"Encoding error: {e}")
        sys.exit(1)

    parsed = parse_transactions(raw)
    valid_filtered, invalid_count, summary = validate_and_filter(parsed)

    # Output file to output/ (note: out_name matches your request)
    out_path = save_cleaned_data(valid_filtered, output_dir, 'Sales_Cleaned_data.txt')

    # Summary of all the operations
    print("\n=== Summary ===")
    print(f"Original records read: {summary['total_input']}")
    print(f"Invalid records: {summary['invalid']}")
    print(f"Final cleaned records: {summary['final_count']}")
    print(f"Cleaned file saved to: {out_path}")

    # Show first 10 valid transactions as a quick sanity check
    print("\n[First 10 valid records]")
    for txn in valid_filtered[:10]:
        print(txn)