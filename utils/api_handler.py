import requests
import json
import os
import re
from pathlib import Path

# Define base directory dynamically
BASE_DIR = Path(__file__).parent.parent
OUTPUT_DIR = BASE_DIR / "output"
DATA_DIR = BASE_DIR / "Data"

def fetch_all_products():

    api_url = "https://dummyjson.com/products?limit=100&skip=0"
    try:
        print("Fetching products from API...")
        response = requests.get(api_url, timeout=10, verify=True)
        response.raise_for_status()
        data = response.json()
        products = data.get('products', [])
        print(f"Successfully fetched {len(products)} products from API.")
        return products
    except requests.exceptions.RequestException as e:
        print(f"API fetch failed: {str(e)}")
        return []

def create_product_mapping(api_products):
    
    mapping = {}
    for product in api_products:
        pid = product.get('id')
        if pid:
            mapping[pid] = {
                'title': product.get('title'),
                'category': product.get('category'),
                'brand': product.get('brand'),
                'rating': product.get('rating'),
                'price': product.get('price')
            }
    print(f"Created product mapping with {len(mapping)} entries.")
    return mapping

def read_sales_data(file_path):
    """
    Reads sales data from txt file and parses into list of dicts
    """
    transactions = []
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        if not lines:
            print("Sales file is empty.")
            return transactions
        
        header = lines[0].strip().split('|')
        for line in lines[1:]:
            values = line.strip().split('|')
            if len(values) == len(header):
                trans = dict(zip(header, values))
                transactions.append(trans)
        print(f"Read {len(transactions)} transactions from sales file.")
        return transactions
    except FileNotFoundError:
        print(f"Sales file not found: {file_path}")
        return []
    except Exception as e:
        print(f"Error reading sales file: {str(e)}")
        return []

def extract_product_id(product_id_str):
    """
    Extract numeric ID from ProductID like P101 -> 101, P5 -> 5
    """
    match = re.search(r'P(\d+)', product_id_str.upper())
    return int(match.group(1)) if match else None

def enrich_sales_data(transactions, product_mapping):
    
    enriched = []
    for trans in transactions:
        enriched_trans = trans.copy()
        pid_str = enriched_trans.get('ProductID', '')
        numeric_id = extract_product_id(pid_str)
        
        if numeric_id and numeric_id in product_mapping:
            info = product_mapping[numeric_id]
            enriched_trans['API_Category'] = info.get('category')
            enriched_trans['API_Brand'] = info.get('brand')
            enriched_trans['API_Rating'] = info.get('rating')
            enriched_trans['API_Match'] = True
            print(f"Matched ProductID {pid_str} -> ID {numeric_id}")
        else:
            enriched_trans['API_Category'] = None
            enriched_trans['API_Brand'] = None
            enriched_trans['API_Rating'] = None
            enriched_trans['API_Match'] = False
            print(f"No match for ProductID {pid_str}")
        
        enriched.append(enriched_trans)
    
    print(f"Enriched {len(enriched)} transactions.")
    return enriched

def save_enriched_data(enriched_transactions, output_path):
    
    if not enriched_transactions:
        print("No data to save.")
        return
    
    # Get all headers including new ones
    headers = list(enriched_transactions[0].keys())
    new_fields = ['API_Category', 'API_Brand', 'API_Rating', 'API_Match']
    for field in new_fields:
        if field not in headers:
            headers.append(field)
    
    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w', encoding='utf-8') as f:
            # Write header
            f.write('|'.join(headers) + '\n')
            # Write data rows
            for trans in enriched_transactions:
                row = []
                for h in headers:
                    val = trans.get(h, '')
                    if val is None:
                        val = ''
                    row.append(str(val))
                f.write('|'.join(row) + '\n')
        print(f"Enriched data saved to: {output_path}")
    except Exception as e:
        print(f"Error saving enriched data: {str(e)}")

def main():
    """
    Main workflow: fetch products -> create mapping -> read sales -> enrich -> save
    """
    print("Starting API integration and data enrichment...")
    
    # Task 1: Fetch products
    api_products = fetch_all_products()
    if not api_products:
        print("Failed to fetch products. Exiting.")
        return
    
    # Task 2: Create mapping
    product_mapping = create_product_mapping(api_products)
    
    # Read sales data
    sales_file = OUTPUT_DIR / "Sales_cleaned_data.txt"
    if not sales_file.exists():
        print(f"Sales file not found: {sales_file}")
        return
    transactions = read_sales_data(sales_file)
    if not transactions:
        print("No transactions to enrich.")
        return
    
    # Task 3: Enrich data
    enriched_data = enrich_sales_data(transactions, product_mapping)
    
    # Task 4: Save enriched data
    enriched_file = DATA_DIR / "enriched_sales_data.txt"
    save_enriched_data(enriched_data, enriched_file)
    
    print("Data enrichment pipeline completed successfully!")

if __name__ == "__main__":
    main()
