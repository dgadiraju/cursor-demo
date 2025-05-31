# CSV to JSON Conversion Project

## Objective
Convert retail database CSV files to JSON format using Python and Pandas.

## Data Source
- **Location**: `retail_db/` directory
- **Files**: 6 CSV files (departments, categories, products, customers, orders, order_items)
- **Format**: Comma-separated values with headers defined in `retail_db/schemas.json`
- **Size**: Approximately 255K total records

## Approach
1. Use Pandas to read CSV files with proper data types based on schema
2. Convert DataFrames to JSON format
3. Save individual JSON files for each table
4. Create a combined JSON file containing all tables

## Expected Output
- **Individual files**: `departments.json`, `categories.json`, `products.json`, `customers.json`, `orders.json`, `order_items.json`
- **Combined file**: `retail_db_combined.json`
- **Output location**: `json_output/` directory

## Tools and Technologies
- Python for scripting
- Pandas Library for CSV/JSON conversion
- Error handling and logging

## Success Criteria
- All CSV data converted to valid JSON format
- Proper data types maintained (integers, floats, strings)
- No data loss during conversion
- Clear documentation and examples provided 