# CSV to JSON Converter

A robust, production-ready Python application for converting retail database CSV files to JSON format with comprehensive data validation and transformation capabilities.

## 🏗️ Architecture

This project follows data engineering best practices with a modular, component-based architecture:

```
src/
├── utils/          # Configuration and logging utilities
├── readers/        # CSV data reading components  
├── processors/     # Data validation and transformation
├── writers/        # JSON output generation
└── main.py         # Main orchestration pipeline
```

## 🚀 Quick Start

### Prerequisites

- Python 3.8+
- Virtual environment support

### Installation

1. **Setup Virtual Environment**
   ```bash
   python -m venv cd-venv
   source cd-venv/bin/activate  # On Windows: cd-venv\Scripts\activate
   ```

2. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Verify Data Structure**
   Ensure your retail database CSV files are in the expected structure:
   ```
   retail_db/
   ├── departments/part-00000
   ├── categories/part-00000
   ├── orders/part-00000
   ├── products/part-00000
   ├── customers/part-00000
   └── order_items/part-00000
   ```

### Usage

#### Option 1: Run Complete Conversion
```bash
python -m src.main
```

#### Option 2: Test the Pipeline
```bash
python test_conversion.py
```

#### Option 3: Use as a Module
```python
from src.main import CSVToJSONConverter

converter = CSVToJSONConverter()
results = converter.run_conversion()
converter.print_final_report(results)
```

## 📊 Data Processing Pipeline

The conversion follows a 5-phase approach:

### Phase 1: Configuration
- Loads schemas and settings from YAML files
- Validates configuration integrity
- Prepares output directories

### Phase 2: CSV Reading
- Reads CSV files with schema-based data types
- Handles missing values and data type conversions
- Provides detailed read statistics

### Phase 3: Data Validation
- Validates data integrity and completeness
- Checks required fields and data types
- Generates comprehensive quality reports

### Phase 4: Data Transformation  
- Transforms data to JSON-ready format
- Handles pandas-specific types and null values
- Generates metadata for each table

### Phase 5: JSON Writing
- Writes individual table JSON files
- Creates combined dataset file
- Validates output file integrity

## 📁 Output Structure

The converter generates:

### Individual Files
```
data/output/
├── departments.json
├── categories.json
├── orders.json
├── products.json
├── customers.json
└── order_items.json
```

### Combined File
```
data/output/retail_db_combined.json
```

### JSON Format
Each file contains:
```json
{
  "metadata": {
    "table_name": "departments",
    "record_count": 6,
    "generated_at": "2024-12-19T...",
    "data_types": {...},
    "statistics": {...}
  },
  "data": [
    {"department_id": 1, "department_name": "Management"},
    ...
  ]
}
```

## ⚙️ Configuration

### Schema Definition (`config/schemas.yaml`)
Defines table structures, data types, and validation rules:
```yaml
tables:
  departments:
    columns:
      department_id:
        type: "int64"
        required: true
        position: 1
      department_name:
        type: "string"
        required: true
        position: 2
```

### Application Settings (`config/settings.yaml`)
Controls processing behavior:
```yaml
input:
  base_path: "data/input/retail_db"
  file_pattern: "part-00000"

output:
  base_path: "data/output"
  individual_files: true
  combined_file: true

processing:
  validate_data: true
  chunk_size: 10000
```

## 🔧 Components

### Utils
- **ConfigManager**: YAML configuration loading and management
- **Logger**: Centralized logging with file/console output

### Readers  
- **SchemaReader**: Schema parsing and pandas dtype generation
- **CSVReader**: Schema-aware CSV reading with proper data types

### Processors
- **DataValidator**: Comprehensive data quality validation
- **DataTransformer**: JSON-ready data transformation

### Writers
- **JSONWriter**: Formatted JSON output with error handling

## 📋 Features

### Data Quality
- ✅ Schema-based validation
- ✅ Missing value handling
- ✅ Data type verification
- ✅ Quality scoring system

### Performance
- ✅ Efficient pandas operations
- ✅ Memory-conscious processing
- ✅ Detailed performance metrics

### Reliability
- ✅ Comprehensive error handling
- ✅ Detailed logging
- ✅ Output validation
- ✅ Graceful failure handling

### Flexibility
- ✅ Configuration-driven processing
- ✅ Modular component architecture
- ✅ Optional validation steps
- ✅ Custom output paths

## 📈 Sample Output

```
================================================================================
CSV TO JSON CONVERSION - FINAL REPORT
================================================================================
Status: SUCCESS
Duration: 2.45 seconds
Tables Processed: 6
Total Rows: 255,379
Files Written: 6
Combined File: Yes

Output Directory: data/output

Individual Files:
  • departments: data/output/departments.json
  • categories: data/output/categories.json
  • orders: data/output/orders.json
  • products: data/output/products.json
  • customers: data/output/customers.json
  • order_items: data/output/order_items.json

Combined File: data/output/retail_db_combined.json
================================================================================
```

## 🧪 Testing

Run comprehensive tests:
```bash
python -m pytest tests/ -v
```

Individual component testing:
```bash
# Test configuration loading
python -c "from src.utils.config import config_manager; print(config_manager.load_settings())"

# Test schema reading  
python -c "from src.readers.schema_reader import schema_reader; print(schema_reader.get_available_tables())"

# Test CSV reading
python -c "from src.readers.csv_reader import csv_reader; df = csv_reader.read_csv_file('departments'); print(df.info())"
```

## 🚦 Error Handling

The application provides detailed error information:
- Configuration errors with specific file/line details
- Data validation issues with affected records
- File I/O problems with permissions and paths
- Transformation errors with data type conflicts

## 📝 Logging

Logs are written to:
- **Console**: Real-time progress and status
- **File**: `logs/conversion.log` with detailed operations

Log levels: DEBUG, INFO, WARNING, ERROR

## 🔄 Workflow Integration

The converter can be integrated into data pipelines:
```python
# Programmatic usage
converter = CSVToJSONConverter()
results = converter.run_conversion(validate_data=False)

if results['success']:
    print(f"Converted {results['summary']['total_rows_read']} rows")
    # Continue with downstream processing
```

## 🛠️ Development

### Project Structure
```
├── config/           # Configuration files
├── data/            # Input/output data directories
├── logs/            # Application logs
├── src/             # Source code
├── tests/           # Test files
├── requirements.txt # Python dependencies
└── README.md       # This file
```

### Design Principles
- **Separation of Concerns**: Each component has a single responsibility
- **Configuration-Driven**: Behavior controlled via YAML files
- **Error Resilience**: Graceful handling of failures
- **Observability**: Comprehensive logging and metrics

## 📋 License

This project is designed for educational and internal use as part of data engineering best practices demonstration.
