# sales-analytics-system/main.py

from pathlib import Path
import sys

from utils.file_handler import (
    read_sales_data,
    parse_transactions,
    validate_and_filter,
    save_cleaned_data,
    output_dir as FH_OUTPUT_DIR,
    data_dir as FH_DATA_DIR,
)

from utils.data_processor import (
    load_cleaned_transactions,
    calculate_total_revenue,
    region_wise_sales,
    top_selling_products,
    customer_analysis,
    daily_sales_trend,
    find_peak_sales_day,
    low_performing_products,
    print_region_summary_table,
    print_top_products_table,
    print_customer_summary_table,
    print_daily_trend_table,
    get_output_dir as DP_get_output_dir,
    export_region_csv,
    export_top_products_csv,
    export_customers_csv,
    export_daily_trend_csv,
    export_low_products_csv,
)

from utils.api_handler import (
    fetch_all_products,
    create_product_mapping,
    read_sales_data as api_read_sales_data,
    enrich_sales_data,
    save_enriched_data,
    DATA_DIR as API_DATA_DIR,
    OUTPUT_DIR as API_OUTPUT_DIR,
)

from utils.sales_report_generation import SalesReportGenerator


def ask_yes_no(prompt: str) -> bool:
    """Simple y/n prompt with validation."""
    while True:
        ans = input(prompt).strip().lower()
        if ans in ("y", "yes"):
            return True
        if ans in ("n", "no"):
            return False
        print("Please enter 'y' or 'n'.")


def ask_float_or_blank(prompt: str):
    """Ask for a float; allow blank to mean None."""
    while True:
        raw = input(prompt).strip()
        if raw == "":
            return None
        try:
            return float(raw)
        except ValueError:
            print("Please enter a valid number or leave blank to skip.")


def main():
    
    print("========================================")
    print("SALES ANALYTICS SYSTEM")
    print("========================================")
    print("Welcome to Sales Analytics System developed by Vikas Pandey for Graded Assignment")
    print()

    # Resolve project paths dynamically (no hardcoding)
    base_dir = Path(__file__).parent
    data_dir = base_dir / "Data"
    output_dir = base_dir / "output"
    data_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    raw_sales_file = data_dir / "sales_data.txt"
    cleaned_sales_file = output_dir / "Sales_cleaned_data.txt"
    enriched_sales_file = data_dir / "enriched_sales_data.txt"
    report_file = output_dir / "sales_report.txt"

    try:
        # [1/10] Read sales data
        print("[1/10] Reading sales data...")
        raw_lines = read_sales_data(raw_sales_file)
        print(f"✓ Successfully read {len(raw_lines)} transactions")

        # [2/10] Parse and clean data
        print("[2/10] Parsing and cleaning data...")
        transactions = parse_transactions(raw_lines)
        print(f"✓ Parsed {len(transactions)} records")

        # [3/10] Filter options
        print("[3/10] Filter Options Available:")
        # validate_and_filter already prints available regions and amount range
        # so first call it with no filters to display those
        temp_filtered, temp_invalid_count, temp_summary = validate_and_filter(transactions)
        # Now ask user if they want to filter
        want_filter = ask_yes_no("Do you want to filter data? (y/n): ")

        region = None
        min_amount = None
        max_amount = None

        if want_filter:
            region = input(
                "Enter region to filter by (leave blank for no region filter): "
            ).strip() or None
            min_amount = ask_float_or_blank(
                "Enter minimum transaction amount (leave blank for no minimum): "
            )
            max_amount = ask_float_or_blank(
                "Enter maximum transaction amount (leave blank for no maximum): "
            )

        # [4/10] Validate transactions with final filters
        print("[4/10] Validating transactions...")
        filtered_transactions, invalid_count, summary = validate_and_filter(
            transactions,
            region=region,
            min_amount=min_amount,
            max_amount=max_amount,
        )
        print(f"✓ Valid: {summary['final_count']} | Invalid: {summary['invalid']}")

        # Save cleaned/filtered data
        cleaned_path = save_cleaned_data(filtered_transactions, output_dir, "Sales_cleaned_data.txt")

        # [5/10] Analyzing sales data (Part 2)
        print("[5/10] Analyzing sales data...")
        # Re-load using data_processor loader to keep behavior consistent
        dp_transactions = load_cleaned_transactions(cleaned_path)

        total_rev = calculate_total_revenue(dp_transactions)
        region_stats = region_wise_sales(dp_transactions)
        top_products = top_selling_products(dp_transactions, n=5)
        customer_stats = customer_analysis(dp_transactions)
        trend = daily_sales_trend(dp_transactions)
        peak_day = find_peak_sales_day(dp_transactions)
        low_products = low_performing_products(dp_transactions, threshold=10)

        # Print nicely using existing helper functions
        print("\n=== Analysis Summary ===")
        print(f"Total Revenue: {total_rev:,.2f}")
        print("\n--- Region-wise Sales ---")
        print_region_summary_table(region_stats)
        print("\n--- Top 5 Products ---")
        print_top_products_table(top_products)
        print("\n--- Top Customers (Top 10) ---")
        print_customer_summary_table(customer_stats, top_n=10)
        print("\n--- Daily Sales Trend (first 15 days) ---")
        print_daily_trend_table(trend, max_rows=15)
        print("\n--- Peak Sales Day ---")
        print(f"{peak_day[0]} | Revenue: {peak_day[1]:,.2f} | Transactions: {peak_day[2]}")
        print("\n--- Low Performing Products (qty < 10) ---")
        if low_products:
            print_top_products_table([(n, q, r) for n, q, r in low_products])
        else:
            print("No products below the specified threshold.")
        print("✓ Analysis complete")

        # Optionally export CSVs (not strictly required by prompt, but consistent with module)
        out_dir_for_dp = DP_get_output_dir()
        export_region_csv(region_stats, out_dir_for_dp)
        export_top_products_csv(top_products, out_dir_for_dp)
        export_customers_csv(customer_stats, out_dir_for_dp)
        export_daily_trend_csv(trend, out_dir_for_dp)
        export_low_products_csv(low_products, out_dir_for_dp)

        # [6/10] Fetch product data from API
        print("[6/10] Fetching product data from API...")
        api_products = fetch_all_products()
        if not api_products:
            print("⚠ Could not fetch products from API. Continuing without enrichment.")
            enriched_path = None
        else:
            print(f"✓ Fetched {len(api_products)} products")

            # [7/10] Enriching sales data
            print("[7/10] Enriching sales data...")
            product_mapping = create_product_mapping(api_products)

            # Read the cleaned data via api_handler helper
            api_sales_file = API_OUTPUT_DIR / "Sales_cleaned_data.txt"
            transactions_for_api = api_read_sales_data(api_sales_file)
            if not transactions_for_api:
                print("⚠ No transactions found to enrich.")
                enriched_path = None
            else:
                enriched_data = enrich_sales_data(transactions_for_api, product_mapping)
                # [8/10] Saving enriched data
                print("[8/10] Saving enriched data...")
                enriched_path = API_DATA_DIR / "enriched_sales_data.txt"
                save_enriched_data(enriched_data, enriched_path)
                print(f"✓ Saved to: {enriched_path}")

        # [9/10] Generating report
        print("[9/10] Generating report...")
        generator = SalesReportGenerator()
        report_content = generator.generate_report()
        report_path = generator.save_report(report_content)
        print(f"✓ Report saved to: {report_path}")

        # [10/10] Process Complete
        print("[10/10] Process Complete!")
        print("========================================")
        print("Execution Summary:")
        print(f"  Cleaned data file : {cleaned_path}")
        print(f"  Enriched data file: {enriched_path if 'enriched_path' in locals() and enriched_path else 'Not generated'}")
        print(f"  Report file       : {report_path}")
        print("========================================")

    except FileNotFoundError as e:
        print(f"✗ FILE ERROR: {e}")
        print("Please ensure input files exist in the expected Data/output directories.")
        sys.exit(1)
    except UnicodeDecodeError as e:
        print(f"✗ ENCODING ERROR: {e}")
        print("There was a problem decoding one of the files.")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\n✗ Process interrupted by user.")
        sys.exit(1)
    except Exception as e:
        print(f"✗ UNEXPECTED ERROR: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
