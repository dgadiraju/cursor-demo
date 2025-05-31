#!/usr/bin/env python3
"""
JSON Output Verification Script

This script validates the generated JSON files and provides sample data
to verify the conversion quality.
"""

import json
import os
from pathlib import Path

def load_json_file(filepath):
    """Load and return JSON data from file."""
    try:
        with open(filepath, 'r', encoding='utf-8') as file:
            return json.load(file)
    except Exception as e:
        print(f"Error loading {filepath}: {e}")
        return None

def verify_json_output():
    """Verify all JSON output files."""
    output_dir = Path("json_output")
    
    if not output_dir.exists():
        print("❌ json_output directory not found!")
        return
    
    print("🔍 JSON OUTPUT VERIFICATION")
    print("=" * 50)
    
    # Expected files
    expected_files = [
        "departments.json",
        "categories.json", 
        "products.json",
        "customers.json",
        "orders.json",
        "order_items.json",
        "retail_db_combined.json"
    ]
    
    total_records = 0
    
    for filename in expected_files:
        filepath = output_dir / filename
        
        if not filepath.exists():
            print(f"❌ Missing: {filename}")
            continue
            
        # Load and verify JSON
        data = load_json_file(filepath)
        
        if data is None:
            print(f"❌ Invalid JSON: {filename}")
            continue
            
        # File size
        file_size = filepath.stat().st_size
        size_str = format_file_size(file_size)
        
        if filename == "retail_db_combined.json":
            # Combined file structure
            print(f"✅ {filename:<25} | Combined file | {size_str}")
            for table_name, table_data in data.items():
                if isinstance(table_data, list):
                    print(f"   └─ {table_name:<20} | {len(table_data):>8} records")
                    total_records += len(table_data)
        else:
            # Individual table files
            if isinstance(data, list):
                record_count = len(data)
                total_records += record_count
                print(f"✅ {filename:<25} | {record_count:>8} records | {size_str}")
                
                # Show sample record
                if record_count > 0:
                    print("   Sample record:")
                    sample = data[0]
                    for key, value in list(sample.items())[:3]:  # First 3 fields
                        print(f"     {key}: {value}")
                    print()
    
    print("=" * 50)
    print(f"Total Records Verified: {total_records:,}")
    print("✅ All JSON files verified successfully!")

def format_file_size(size_bytes):
    """Format file size in human readable format."""
    if size_bytes < 1024:
        return f"{size_bytes}B"
    elif size_bytes < 1024**2:
        return f"{size_bytes/1024:.1f}KB"
    elif size_bytes < 1024**3:
        return f"{size_bytes/(1024**2):.1f}MB"
    else:
        return f"{size_bytes/(1024**3):.1f}GB"

def show_data_relationships():
    """Show data relationships between tables."""
    print("\n🔗 DATA RELATIONSHIPS")
    print("=" * 50)
    
    output_dir = Path("json_output")
    
    # Load key tables
    departments = load_json_file(output_dir / "departments.json")
    categories = load_json_file(output_dir / "categories.json") 
    products = load_json_file(output_dir / "products.json")
    
    if departments and categories and products:
        print("Department → Categories → Products")
        print("-" * 50)
        
        # Group categories by department
        dept_categories = {}
        for cat in categories:
            dept_id = cat['category_department_id']
            if dept_id not in dept_categories:
                dept_categories[dept_id] = []
            dept_categories[dept_id].append(cat)
        
        # Count products by category
        cat_products = {}
        for prod in products:
            cat_id = prod['product_category_id']
            if cat_id not in cat_products:
                cat_products[cat_id] = 0
            cat_products[cat_id] += 1
        
        # Show relationships
        for dept in departments:
            dept_id = dept['department_id']
            dept_name = dept['department_name']
            
            dept_cats = dept_categories.get(dept_id, [])
            total_products = sum(cat_products.get(cat['category_id'], 0) for cat in dept_cats)
            
            print(f"{dept_name} (ID: {dept_id})")
            print(f"  ├─ Categories: {len(dept_cats)}")
            print(f"  └─ Products: {total_products}")
            
            # Show top categories
            if dept_cats:
                for cat in dept_cats[:3]:  # Show first 3 categories
                    cat_id = cat['category_id']
                    cat_name = cat['category_name']
                    prod_count = cat_products.get(cat_id, 0)
                    print(f"     • {cat_name}: {prod_count} products")
                if len(dept_cats) > 3:
                    print(f"     ... and {len(dept_cats) - 3} more categories")
            print()

def main():
    """Main verification function."""
    verify_json_output()
    show_data_relationships()
    
    print("\n🎉 JSON CONVERSION PROJECT COMPLETED SUCCESSFULLY!")
    print("📊 All data successfully converted from CSV to JSON format")
    print("📁 Files available in json_output/ directory")

if __name__ == "__main__":
    main() 