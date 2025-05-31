# Implementation Guidelines

## Development Environment Setup

### Python Virtual Environment

**Create and activate virtual environment:**
```bash
# Create virtual environment
python -m venv cd-venv

# Activate virtual environment
# On macOS/Linux:
source cd-venv/bin/activate

# On Windows:
cd-venv\Scripts\activate

# Verify activation
which python  # Should show cd-venv path
```

**Deactivate when done:**
```bash
deactivate
```

### Dependencies Management

**Install required packages:**
```bash
# Install the latest versions with out specifying the version at the time of install (if new library)
pip install pandas
pip install PyYAML
pip install pytest

# Generate requirements.txt with exact versions
pip freeze > requirements.txt
```

**Install from requirements.txt (for team members):**
```bash
pip install -r requirements.txt
```

## Development Workflow

### 1. Branch Strategy
```bash
# Create feature branch
git checkout -b feature/csv-reader-implementation
git checkout -b feature/data-transformer-implementation
git checkout -b feature/json-writer-implementation

# Work on your component
# Commit changes
git add .
git commit -m "feat: implement csv_reader component"

# Push and create PR
git push origin feature/csv-reader-implementation
```

### 2. Code Standards

**File Structure Convention:**
- Use snake_case for file names: `csv_reader.py`, `data_transformer.py`
- Use PascalCase for class names: `CSVReader`, `DataTransformer`
- Use snake_case for function names: `read_csv_file()`, `transform_data()`

**Import Organization:**
```python
# Standard library imports
import os
import logging
from pathlib import Path

# Third-party imports
import pandas as pd
import yaml

# Local imports
from src.utils.config import ConfigManager
from src.utils.logger import setup_logger
```

### 3. Configuration Management

**Always use configuration files:**
```python
# ✅ Good - Use config
config = ConfigManager.load_config()
input_path = config['input']['base_path']

# ❌ Bad - Hardcode paths
input_path = "retail_db"
```

### 4. Error Handling Standards

**Consistent error handling pattern:**
```python
import logging

logger = logging.getLogger(__name__)

def read_csv_file(file_path: str):
    try:
        # Implementation
        logger.info(f"Successfully read {file_path}")
        return data
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error reading {file_path}: {e}")
        raise
```

## Testing Guidelines

### 1. Test Structure
```bash
# Run all tests
pytest tests/

# Run specific test category
pytest tests/test_readers/
pytest tests/test_processors/
pytest tests/test_writers/

# Run with coverage
pytest --cov=src tests/
```

### 2. Test Data Management
- Place sample data in `tests/fixtures/`
- Use small datasets (< 100 rows) for unit tests
- Name test files clearly: `test_departments.csv`, `test_categories.csv`

### 3. Test Naming Convention
```python
# Test file: test_csv_reader.py
def test_read_valid_csv_file():
    # Test successful reading
    pass

def test_read_missing_csv_file():
    # Test error handling
    pass

def test_read_csv_with_schema():
    # Test schema application
    pass
```

## Code Quality Standards

### 1. Type Hints
```python
from typing import Dict, List, Any
import pandas as pd

def read_csv_file(file_path: str, schema: Dict[str, str]) -> pd.DataFrame:
    """Read CSV file with specified schema."""
    pass
```

### 2. Documentation
```python
def transform_data(df: pd.DataFrame, schema: Dict[str, Any]) -> pd.DataFrame:
    """
    Transform DataFrame according to schema specifications.
    
    Args:
        df: Input DataFrame from CSV
        schema: Schema configuration with data types
        
    Returns:
        Transformed DataFrame ready for JSON conversion
        
    Raises:
        ValueError: If required columns are missing
    """
    pass
```

### 3. Logging Standards
```python
# Use appropriate log levels
logger.debug("Processing started")      # Development info
logger.info("Processing 1000 records")  # Normal operations
logger.warning("Empty column found")    # Potential issues
logger.error("File not found")          # Errors
```

## Component Development Order

### Recommended Implementation Sequence:
1. **Utils** (`src/utils/`) - Foundation components
2. **Readers** (`src/readers/`) - Data input layer
3. **Processors** (`src/processors/`) - Data transformation layer
4. **Writers** (`src/writers/`) - Data output layer
5. **Main** (`src/main.py`) - Orchestration
6. **Tests** - Unit and integration tests

### Component Dependencies:
- Readers depend on: Utils
- Processors depend on: Utils, Readers
- Writers depend on: Utils
- Main depends on: All components

## Collaboration Guidelines

### 1. Communication
- Document any design decisions in code comments
- Update relevant markdown files when changing interfaces
- Notify team of breaking changes in component interfaces

### 2. Code Reviews
- Each component should be reviewed before merging
- Focus review on: error handling, type safety, test coverage
- Verify component follows design document specifications

### 3. Integration Points
**Component Interfaces:**
```python
# csv_reader.py output format
def read_csv_file() -> pd.DataFrame:
    pass

# data_transformer.py input/output format  
def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    pass

# json_writer.py input format
def write_json_files(df: pd.DataFrame, table_name: str) -> None:
    pass
```

### 4. Environment Consistency
- Always work within `cd-venv` virtual environment
- Use exact library versions from `requirements.txt`
- Test with same Python version (document in README)

## Troubleshooting

### Common Issues:
```bash
# Virtual environment not activated
which python  # Should show cd-venv path

# Wrong dependencies
pip list  # Verify installed packages

# Import errors
export PYTHONPATH="${PYTHONPATH}:$(pwd)"  # Add project root to path
```

### Debug Mode:
```python
# Enable debug logging in config/settings.yaml
logging:
  level: "DEBUG"
```

## Pre-Commit Checklist

Before committing code:
- [ ] Virtual environment `cd-venv` is activated
- [ ] All tests pass: `pytest tests/`
- [ ] Code follows naming conventions
- [ ] Type hints are added
- [ ] Docstrings are complete
- [ ] Error handling is implemented
- [ ] Logging statements are appropriate
- [ ] No hardcoded paths or values 