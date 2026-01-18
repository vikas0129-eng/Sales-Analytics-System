"""
Sales Report Generation Module

Generates comprehensive formatted text report from enriched sales data.
Integrates with file_handler.py, data_processor.py, and api_handler.py.
"""

import csv
from pathlib import Path
from typing import List, Dict, Tuple
from datetime import datetime
from collections import defaultdict


class SalesReportGenerator:
    """
    Generates comprehensive formatted text reports from cleaned sales transactions.
    """
    
    def __init__(self, data_file: Path = None):
        """
        Initialize report generator with dynamic path resolution.
        
        Args:
            data_file: Path to cleaned sales data. If None, uses default location.
        """
        # Dynamic path resolution (not hardcoded)
        base_dir = Path(__file__).parent.parent
        self.output_dir = base_dir / "output"
        self.data_dir = base_dir / "Data"
        
        # Create directories if they don't exist
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # Use provided file or default location
        self.data_file = Path(data_file) if data_file else self.output_dir / "Sales_cleaned_data.txt"
        self.enriched_file = self.data_dir / "enriched_sales_data.txt"
        self.report_file = self.output_dir / "sales_report.txt"
        
        self.transactions = []
        self.enriched_data = []
        
    def _load_transactions(self) -> List[Dict]:
        """
        Load transactions from cleaned data file with error handling.
        
        Returns:
            List of transaction dictionaries
        """
        if not self.data_file.exists():
            raise FileNotFoundError(f"Data file not found: {self.data_file}")
        
        transactions = []
        encodings = ['utf-8', 'latin-1', 'cp1252']
        
        for enc in encodings:
            try:
                with self.data_file.open('r', encoding=enc, newline='') as f:
                    reader = csv.reader(f, delimiter='|')
                    rows = list(reader)
                break
            except UnicodeDecodeError:
                continue
        else:
            raise UnicodeDecodeError("read", b"", 0, 1, 
                                   f"Unable to decode file with encodings {encodings}")
        
        if not rows:
            print("⚠️  Warning: Data file is empty")
            return []
        
        # Skip header row
        header = rows[0]
        for row in rows[1:]:
            if len(row) >= 8:
                # Pad row if needed
                while len(row) < len(header):
                    row.append('')
                
                trans = {}
                for i, h in enumerate(header):
                    trans[h] = row[i]
                
                # Parse numeric fields
                try:
                    trans['Quantity'] = int(trans.get('Quantity', 0))
                    trans['UnitPrice'] = float(trans.get('UnitPrice', 0))
                    trans['Amount'] = float(trans.get('Amount', 0))
                except ValueError:
                    continue
                
                transactions.append(trans)
        
        print(f"✓ Loaded {len(transactions)} transactions from {self.data_file.name}")
        return transactions
    
    def _load_enriched_data(self) -> List[Dict]:
        """Load enriched API data if available."""
        if not self.enriched_file.exists():
            print(f"ℹ️  Enriched data not available: {self.enriched_file}")
            return []
        
        enriched = []
        try:
            with self.enriched_file.open('r', encoding='utf-8', newline='') as f:
                reader = csv.DictReader(f, delimiter='|')
                enriched = list(reader)
            print(f"✓ Loaded {len(enriched)} enriched records")
        except Exception as e:
            print(f"⚠️  Could not load enriched data: {e}")
        
        return enriched
    
    def _format_currency(self, amount: float) -> str:
        """Format amount as Indian Rupees with commas."""
        # Format with 2 decimal places and commas
        return f"₹{amount:,.2f}"
    
    def _calculate_summary_metrics(self) -> Dict:
        """Calculate overall summary statistics."""
        if not self.transactions:
            return {}
        
        total_revenue = sum(t.get('Amount', 0) for t in self.transactions)
        total_transactions = len(self.transactions)
        avg_order_value = total_revenue / total_transactions if total_transactions > 0 else 0
        
        # Get date range
        dates = [t.get('Date', '') for t in self.transactions if t.get('Date')]
        dates = sorted([d for d in dates if d])
        
        date_range = f"{dates[0]} to {dates[-1]}" if dates else "N/A"
        
        return {
            'total_revenue': total_revenue,
            'total_transactions': total_transactions,
            'avg_order_value': avg_order_value,
            'date_range': date_range
        }
    
    def _calculate_region_performance(self) -> Dict[str, Dict]:
        """Calculate region-wise sales performance."""
        region_stats = defaultdict(lambda: {'sales': 0, 'count': 0})
        
        for trans in self.transactions:
            region = trans.get('Region', 'Unknown')
            amount = trans.get('Amount', 0)
            region_stats[region]['sales'] += amount
            region_stats[region]['count'] += 1
        
        # Calculate total for percentages
        total_sales = sum(s['sales'] for s in region_stats.values())
        
        # Sort by sales descending
        sorted_regions = sorted(region_stats.items(), 
                              key=lambda x: x[1]['sales'], 
                              reverse=True)
        
        result = {}
        for region, stats in sorted_regions:
            pct = (stats['sales'] / total_sales * 100) if total_sales > 0 else 0
            result[region] = {
                'sales': stats['sales'],
                'percentage': pct,
                'count': stats['count']
            }
        
        return result
    
    def _get_top_products(self, n: int = 5) -> List[Tuple]:
        """Get top N products by quantity sold."""
        product_stats = defaultdict(lambda: {'qty': 0, 'revenue': 0})
        
        for trans in self.transactions:
            prod = trans.get('ProductName', 'Unknown')
            qty = trans.get('Quantity', 0)
            amount = trans.get('Amount', 0)
            product_stats[prod]['qty'] += qty
            product_stats[prod]['revenue'] += amount
        
        sorted_prods = sorted(product_stats.items(), 
                            key=lambda x: x[1]['qty'], 
                            reverse=True)
        
        result = []
        for rank, (prod_name, stats) in enumerate(sorted_prods[:n], 1):
            result.append((rank, prod_name, stats['qty'], stats['revenue']))
        
        return result
    
    def _get_top_customers(self, n: int = 5) -> List[Tuple]:
        """Get top N customers by total spend."""
        customer_stats = defaultdict(lambda: {'spent': 0, 'count': 0})
        
        for trans in self.transactions:
            cust = trans.get('CustomerID', 'Unknown')
            amount = trans.get('Amount', 0)
            customer_stats[cust]['spent'] += amount
            customer_stats[cust]['count'] += 1
        
        sorted_custs = sorted(customer_stats.items(), 
                            key=lambda x: x[1]['spent'], 
                            reverse=True)
        
        result = []
        for rank, (cust_id, stats) in enumerate(sorted_custs[:n], 1):
            result.append((rank, cust_id, stats['spent'], stats['count']))
        
        return result
    
    def _get_daily_trend(self) -> Dict[str, Dict]:
        """Calculate daily sales trend."""
        daily_stats = defaultdict(lambda: {'revenue': 0, 'txn': 0, 'customers': set()})
        
        for trans in self.transactions:
            date = trans.get('Date', 'Unknown')
            amount = trans.get('Amount', 0)
            cust = trans.get('CustomerID', '')
            
            daily_stats[date]['revenue'] += amount
            daily_stats[date]['txn'] += 1
            if cust:
                daily_stats[date]['customers'].add(cust)
        
        # Sort by date
        sorted_dates = sorted(daily_stats.keys())
        
        result = {}
        for date in sorted_dates:
            stats = daily_stats[date]
            result[date] = {
                'revenue': stats['revenue'],
                'txn': stats['txn'],
                'unique_customers': len(stats['customers'])
            }
        
        return result
    
    def _get_best_selling_day(self) -> Tuple[str, float, int]:
        """Find the day with highest revenue."""
        daily = self._get_daily_trend()
        
        if not daily:
            return ('N/A', 0, 0)
        
        best_day = max(daily.items(), key=lambda x: x[1]['revenue'])
        return (best_day[0], best_day[1]['revenue'], best_day[1]['txn'])
    
    def _get_low_performing_products(self, threshold: int = 10) -> List[Tuple]:
        """Get products with quantity below threshold."""
        product_stats = defaultdict(lambda: {'qty': 0, 'revenue': 0})
        
        for trans in self.transactions:
            prod = trans.get('ProductName', 'Unknown')
            qty = trans.get('Quantity', 0)
            amount = trans.get('Amount', 0)
            product_stats[prod]['qty'] += qty
            product_stats[prod]['revenue'] += amount
        
        low_prods = [(name, stats['qty'], stats['revenue']) 
                     for name, stats in product_stats.items() 
                     if stats['qty'] < threshold]
        
        # Sort by quantity ascending
        low_prods.sort(key=lambda x: x[1])
        
        return low_prods
    
    def _get_enrichment_summary(self) -> Dict:
        """Get API enrichment statistics."""
        if not self.enriched_data:
            return {
                'total_enriched': 0,
                'success_rate': 0,
                'failed_products': []
            }
        
        total = len(self.enriched_data)
        matched = sum(1 for e in self.enriched_data if e.get('API_Match') == 'True')
        success_rate = (matched / total * 100) if total > 0 else 0
        
        failed = [e.get('ProductID', 'Unknown') 
                 for e in self.enriched_data 
                 if e.get('API_Match') == 'False']
        
        return {
            'total_enriched': total,
            'matched': matched,
            'success_rate': success_rate,
            'failed_products': failed
        }
    
    def _calculate_region_avg_transaction(self) -> Dict[str, float]:
        """Calculate average transaction value per region."""
        region_stats = defaultdict(lambda: {'sales': 0, 'count': 0})
        
        for trans in self.transactions:
            region = trans.get('Region', 'Unknown')
            amount = trans.get('Amount', 0)
            region_stats[region]['sales'] += amount
            region_stats[region]['count'] += 1
        
        result = {}
        for region, stats in region_stats.items():
            avg = stats['sales'] / stats['count'] if stats['count'] > 0 else 0
            result[region] = avg
        
        return result
    
    def generate_report(self) -> str:
        """
        Generate comprehensive sales report.
        
        Returns:
            Report content as string
        """
        print("\n" + "="*60)
        print("GENERATING COMPREHENSIVE SALES REPORT")
        print("="*60)
        
        # Load data
        print("\n[1/9] Loading transactions...")
        self.transactions = self._load_transactions()
        
        if not self.transactions:
            raise ValueError("No valid transactions to generate report")
        
        print("[2/9] Loading enriched data...")
        self.enriched_data = self._load_enriched_data()
        
        # Build report
        report_lines = []
        
        # SECTION 1: HEADER
        print("[3/9] Generating HEADER section...")
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        report_lines.append("=" * 60)
        report_lines.append("SALES ANALYTICS REPORT")
        report_lines.append(f"Generated: {now}")
        report_lines.append(f"Records Processed: {len(self.transactions)}")
        report_lines.append("=" * 60)
        report_lines.append("")
        
        # SECTION 2: OVERALL SUMMARY
        print("[4/9] Generating OVERALL SUMMARY section...")
        metrics = self._calculate_summary_metrics()
        report_lines.append("OVERALL SUMMARY")
        report_lines.append("-" * 60)
        report_lines.append(f"Total Revenue: {self._format_currency(metrics['total_revenue'])}")
        report_lines.append(f"Total Transactions: {metrics['total_transactions']}")
        report_lines.append(f"Average Order Value: {self._format_currency(metrics['avg_order_value'])}")
        report_lines.append(f"Date Range: {metrics['date_range']}")
        report_lines.append("")
        
        # SECTION 3: REGION-WISE PERFORMANCE
        print("[5/9] Generating REGION-WISE PERFORMANCE section...")
        region_perf = self._calculate_region_performance()
        report_lines.append("REGION-WISE PERFORMANCE")
        report_lines.append("-" * 60)
        report_lines.append(f"{'Region':<20} {'Sales':<20} {'% of Total':<15} {'Transactions':<15}")
        report_lines.append("-" * 60)
        for region, stats in region_perf.items():
            report_lines.append(
                f"{region:<20} {self._format_currency(stats['sales']):<20} "
                f"{stats['percentage']:<14.2f}% {stats['count']:<15}"
            )
        report_lines.append("")
        
        # SECTION 4: TOP 5 PRODUCTS
        print("[6/9] Generating TOP 5 PRODUCTS section...")
        top_prods = self._get_top_products(n=5)
        report_lines.append("TOP 5 PRODUCTS")
        report_lines.append("-" * 60)
        report_lines.append(f"{'Rank':<8} {'Product Name':<30} {'Quantity Sold':<15} {'Revenue':<20}")
        report_lines.append("-" * 60)
        for rank, name, qty, rev in top_prods:
            report_lines.append(
                f"{rank:<8} {name:<30} {qty:<15} {self._format_currency(rev):<20}"
            )
        report_lines.append("")
        
        # SECTION 5: TOP 5 CUSTOMERS
        print("[7/9] Generating TOP 5 CUSTOMERS section...")
        top_custs = self._get_top_customers(n=5)
        report_lines.append("TOP 5 CUSTOMERS")
        report_lines.append("-" * 60)
        report_lines.append(f"{'Rank':<8} {'Customer ID':<20} {'Total Spent':<20} {'Order Count':<15}")
        report_lines.append("-" * 60)
        for rank, cust_id, spent, count in top_custs:
            report_lines.append(
                f"{rank:<8} {cust_id:<20} {self._format_currency(spent):<20} {count:<15}"
            )
        report_lines.append("")
        
        # SECTION 6: DAILY SALES TREND
        print("[8/9] Generating DAILY SALES TREND section...")
        daily_trend = self._get_daily_trend()
        report_lines.append("DAILY SALES TREND")
        report_lines.append("-" * 60)
        report_lines.append(f"{'Date':<15} {'Revenue':<20} {'Transactions':<15} {'Unique Customers':<20}")
        report_lines.append("-" * 60)
        for i, (date, stats) in enumerate(daily_trend.items()):
            if i >= 26:  
                report_lines.append(f"... ({len(daily_trend) - 26} more days)")
                break
            report_lines.append(
                f"{date:<15} {self._format_currency(stats['revenue']):<20} "
                f"{stats['txn']:<15} {stats['unique_customers']:<20}"
            )
        report_lines.append("")
        
        # SECTION 7: PRODUCT PERFORMANCE ANALYSIS
        print("[9/9] Generating PRODUCT PERFORMANCE ANALYSIS section...")
        best_day = self._get_best_selling_day()
        low_prods = self._get_low_performing_products(threshold=10)
        region_avg = self._calculate_region_avg_transaction()
        
        report_lines.append("PRODUCT PERFORMANCE ANALYSIS")
        report_lines.append("-" * 60)
        report_lines.append(f"Best Selling Day: {best_day[0]}")
        report_lines.append(f"  Revenue: {self._format_currency(best_day[1])}")
        report_lines.append(f"  Transactions: {best_day[2]}")
        report_lines.append("")
        
        if low_prods:
            report_lines.append("Low Performing Products (qty < 10):")
            report_lines.append(f"{'Product Name':<30} {'Total Qty':<15} {'Total Revenue':<20}")
            report_lines.append("-" * 60)
            for prod, qty, rev in low_prods:
                report_lines.append(
                    f"{prod:<30} {qty:<15} {self._format_currency(rev):<20}"
                )
        else:
            report_lines.append("Low Performing Products: None")
        
        report_lines.append("")
        report_lines.append("Average Transaction Value per Region:")
        report_lines.append(f"{'Region':<20} {'Average Value':<20}")
        report_lines.append("-" * 60)
        for region, avg_val in sorted(region_avg.items()):
            report_lines.append(f"{region:<20} {self._format_currency(avg_val):<20}")
        report_lines.append("")
        
        # SECTION 8: API ENRICHMENT SUMMARY
        print("[10/11] Generating API ENRICHMENT SUMMARY section...")
        enrichment = self._get_enrichment_summary()
        report_lines.append("API ENRICHMENT SUMMARY")
        report_lines.append("-" * 60)
        report_lines.append(f"Total Products Enriched: {enrichment['total_enriched']}")
        report_lines.append(f"Successfully Matched: {enrichment['matched']}")
        report_lines.append(f"Success Rate: {enrichment['success_rate']:.2f}%")
        
        if enrichment['failed_products']:
            report_lines.append("")
            report_lines.append("Products That Couldn't Be Enriched:")
            for prod_id in enrichment['failed_products'][:10]:  # Show first 10
                report_lines.append(f"  - {prod_id}")
            if len(enrichment['failed_products']) > 10:
                report_lines.append(f"  ... and {len(enrichment['failed_products']) - 10} more")
        else:
            report_lines.append("All products successfully enriched!")
        
        report_lines.append("")
        report_lines.append("=" * 60)
        report_lines.append("END OF REPORT")
        report_lines.append("=" * 60)
        
        report_content = "\n".join(report_lines)
        print("[11/11] Report generation complete!")
        
        return report_content
    
    def save_report(self, report_content: str = None) -> Path:
        """
        Save report to file.
        
        Args:
            report_content: Report text. If None, generates new report.
        
        Returns:
            Path to saved report file
        """
        if report_content is None:
            report_content = self.generate_report()
        
        try:
            self.report_file.parent.mkdir(parents=True, exist_ok=True)
            
            with self.report_file.open('w', encoding='utf-8') as f:
                f.write(report_content)
            
            print(f"\n✓ Report successfully saved to: {self.report_file}")
            print(f"  File size: {self.report_file.stat().st_size:,} bytes")
            
            return self.report_file
        
        except Exception as e:
            print(f"✗ Error saving report: {e}")
            raise


def main():
    """Main entry point for report generation."""
    try:
        print("\n" + "="*60)
        print("SALES ANALYTICS REPORT GENERATOR")
        print("="*60)
        
        # Initialize generator
        generator = SalesReportGenerator()
        
        # Generate and save report
        report_content = generator.generate_report()
        report_path = generator.save_report(report_content)
        
        # Print first 30 lines as preview
        print("\n" + "="*60)
        print("REPORT PREVIEW (First 30 lines)")
        print("="*60)
        lines = report_content.split('\n')
        for line in lines[:30]:
            print(line)
        if len(lines) > 30:
            print(f"\n... ({len(lines) - 30} more lines)")
        
        print("\n" + "="*60)
        print("✓ REPORT GENERATION COMPLETED SUCCESSFULLY")
        print("="*60)
        
    except FileNotFoundError as e:
        print(f"\n✗ FILE ERROR: {e}")
        print("  Please ensure Sales_cleaned_data.txt exists in the output directory.")
        exit(1)
    
    except ValueError as e:
        print(f"\n✗ DATA ERROR: {e}")
        exit(1)
    
    except Exception as e:
        print(f"\n✗ UNEXPECTED ERROR: {e}")
        import traceback
        traceback.print_exc()
        exit(1)


if __name__ == "__main__":
    main()
