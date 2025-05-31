# CSV to JSON Conversion - Design Document

## Design Principles
- **Separation of Duties**: Clear distinction between read, process, and write operations
- **Configurability**: External configuration for file paths, schemas, and settings
- **Testability**: Modular design with comprehensive test coverage
- **Maintainability**: Clean code structure following industry standards
- **Error Handling**: Robust error handling and logging at each stage

## Folder Structure

```
csv-to-json-converter/
├── src/                          # Source code
│   ├── readers/                  # Data reading components
│   │   ├── __init__.py
│   │   ├── csv_reader.py         # CSV file reading logic
│   │   └── schema_reader.py      # Schema definition reader
│   ├── processors/               # Data processing components
│   │   ├── __init__.py
│   │   ├── data_validator.py     # Data validation logic
│   │   └── data_transformer.py   # Data type conversion logic
│   ├── writers/                  # Data writing components
│   │   ├── __init__.py
│   │   └── json_writer.py        # JSON file writing logic
│   ├── utils/                    # Utility functions
│   │   ├── __init__.py
│   │   ├── logger.py             # Logging configuration
│   │   └── config.py             # Configuration management
│   └── main.py                   # Main orchestration script
├── config/                       # Configuration files
│   ├── settings.yaml             # Application settings
│   └── schemas.yaml              # Data schemas (converted from JSON)
├── tests/                        # Test files
│   ├── __init__.py
│   ├── test_readers/
│   ├── test_processors/
│   ├── test_writers/
│   ├── test_integration/
│   └── fixtures/                 # Test data samples
├── data/                         # Data directories
│   ├── input/                    # Input CSV files (symlink to retail_db)
│   └── output/                   # Output JSON files
├── logs/                         # Application logs
├── requirements.txt              # Python dependencies
└── README.md                     # Usage instructions
```

## Component Design

### 1. Readers (`src/readers/`)

#### `csv_reader.py`
- **Purpose**: Read CSV files using Pandas with proper data types
- **Input**: File path, schema configuration
- **Output**: Pandas DataFrame
- **Responsibilities**:
  - Load CSV files with appropriate data types
  - Handle missing or malformed data
  - Apply basic data validation during read

#### `schema_reader.py`
- **Purpose**: Load and parse schema definitions
- **Input**: Schema configuration file
- **Output**: Schema dictionary
- **Responsibilities**:
  - Read schema from YAML/JSON
  - Validate schema structure
  - Provide schema lookup functions

### 2. Processors (`src/processors/`)

#### `data_validator.py`
- **Purpose**: Validate data integrity and completeness
- **Input**: Pandas DataFrame, schema
- **Output**: Validation results, cleaned DataFrame
- **Responsibilities**:
  - Check for required fields
  - Validate data types
  - Identify and report data quality issues
  - Apply business rules validation

#### `data_transformer.py`
- **Purpose**: Transform data types and structure for JSON output
- **Input**: Pandas DataFrame, schema
- **Output**: Transformed DataFrame ready for JSON conversion
- **Responsibilities**:
  - Apply data type conversions
  - Handle null values appropriately
  - Format data for JSON serialization

### 3. Writers (`src/writers/`)

#### `json_writer.py`
- **Purpose**: Write DataFrames to JSON files
- **Input**: Pandas DataFrame, output configuration
- **Output**: JSON files (individual and combined)
- **Responsibilities**:
  - Convert DataFrame to JSON format
  - Write individual table files
  - Create combined JSON structure
  - Ensure proper JSON formatting

### 4. Utilities (`src/utils/`)

#### `config.py`
- **Purpose**: Centralized configuration management
- **Responsibilities**:
  - Load configuration from YAML files
  - Provide configuration access methods
  - Environment-specific settings

#### `logger.py`
- **Purpose**: Logging setup and management
- **Responsibilities**:
  - Configure logging levels and formats
  - Set up file and console logging
  - Provide logging utilities

## Configuration Design

### `config/settings.yaml`
```yaml
input:
  base_path: "retail_db"
  file_pattern: "part-00000"
  
output:
  base_path: "data/output"
  individual_files: true
  combined_file: true
  combined_filename: "retail_db_combined.json"

processing:
  chunk_size: 10000  # For large files
  validate_data: true
  
logging:
  level: "INFO"
  file: "logs/conversion.log"
  format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
```

### `config/schemas.yaml`
```yaml
tables:
  departments:
    columns:
      department_id: {type: "int64", required: true}
      department_name: {type: "string", required: true}
  
  categories:
    columns:
      category_id: {type: "int64", required: true}
      category_department_id: {type: "int64", required: true}
      category_name: {type: "string", required: true}
  # ... other tables
```

## Data Flow Design

1. **Initialization**
   - Load configuration from `settings.yaml`
   - Load schemas from `schemas.yaml`
   - Setup logging

2. **Read Phase**
   - For each table in schema:
     - Read CSV file using `csv_reader`
     - Apply schema-based data types

3. **Process Phase**
   - Validate data using `data_validator`
   - Transform data using `data_transformer`
   - Log any data quality issues

4. **Write Phase**
   - Write individual JSON files using `json_writer`
   - Create combined JSON file
   - Generate conversion summary report

## Test Strategy

### Test Categories

#### Unit Tests (`tests/test_*/`)
- **Reader Tests**: CSV reading with various data types and edge cases
- **Processor Tests**: Data validation and transformation logic
- **Writer Tests**: JSON output formatting and file creation
- **Utility Tests**: Configuration loading and logging functionality

#### Integration Tests (`tests/test_integration/`)
- **End-to-End**: Complete conversion pipeline with sample data
- **Error Scenarios**: Handling of malformed CSV files
- **Large Dataset**: Performance testing with substantial data volumes

#### Test Fixtures (`tests/fixtures/`)
- **Sample CSV Files**: Small datasets for each table type
- **Invalid Data**: Files with missing fields, wrong types, empty values
- **Expected JSON**: Reference outputs for validation

### Test Cases

#### 1. Happy Path Tests
- Convert valid CSV files with all data types
- Verify JSON structure and data integrity
- Check individual and combined file outputs

#### 2. Error Handling Tests
- Missing CSV files
- Invalid data types in CSV
- Empty or corrupted files
- Schema mismatches

#### 3. Data Quality Tests
- Missing required fields
- Null value handling
- Large number handling (precision)
- Unicode character support

#### 4. Performance Tests
- Large file processing
- Memory usage validation
- Processing time benchmarks

## Error Handling Strategy

### Error Categories
1. **Configuration Errors**: Missing config files, invalid settings
2. **Input Errors**: Missing CSV files, schema mismatches
3. **Processing Errors**: Data validation failures, transformation issues
4. **Output Errors**: File write permissions, disk space issues

### Error Response
- **Graceful Degradation**: Continue processing other tables if one fails
- **Detailed Logging**: Log errors with context and suggestions
- **Summary Reporting**: Provide conversion summary with error counts
- **Exit Codes**: Return appropriate exit codes for automation

## Monitoring and Logging

### Log Levels
- **INFO**: Normal processing progress
- **WARNING**: Data quality issues, recoverable errors
- **ERROR**: Processing failures, unrecoverable errors
- **DEBUG**: Detailed processing information (development)

### Metrics to Track
- Processing time per table
- Record counts (input vs output)
- Data quality issues count
- File sizes (input vs output)
- Memory usage patterns

## Scalability Considerations

### Current Scope
- Single machine processing
- In-memory data processing using Pandas
- File-based input/output

### Future Enhancements
- Chunked processing for very large files
- Parallel processing for multiple tables
- Database connectivity options
- Cloud storage integration

## Success Criteria

1. **Functional**: All CSV data successfully converted to JSON
2. **Quality**: Zero data loss, proper type conversion
3. **Reliability**: Robust error handling and recovery
4. **Maintainability**: Clean, testable, and documented code
5. **Performance**: Efficient processing within reasonable time limits 