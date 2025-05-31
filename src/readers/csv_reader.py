"""
CSV reading utilities.

This module provides functionality to read CSV files using Pandas
with proper data type handling based on schema configuration.
"""

from typing import Dict, Any, Optional
import pandas as pd
from pathlib import Path
from src.utils.config import config_manager
from src.utils.logger import get_logger
from src.readers.schema_reader import schema_reader

logger = get_logger(__name__)


class CSVReader:
    """Handles reading CSV files with schema-based data types."""
    
    def __init__(self):
        """Initialize CSVReader."""
        self.config = config_manager
        self.schema_reader = schema_reader
    
    def read_csv_file(self, table_name: str, file_path: Optional[str] = None) -> pd.DataFrame:
        """
        Read CSV file for a specific table with proper data types.
        
        Args:
            table_name: Name of the table to read
            file_path: Optional custom file path. If None, uses config path
            
        Returns:
            Pandas DataFrame with proper data types
            
        Raises:
            FileNotFoundError: If CSV file is not found
            ValueError: If schema validation fails
            pd.errors.EmptyDataError: If CSV file is empty
        """
        # Validate schema first
        self.schema_reader.validate_schema(table_name)
        
        # Determine file path
        if file_path is None:
            file_path = self._get_table_file_path(table_name)
        
        file_path = Path(file_path)
        
        # Check if file exists
        if not file_path.exists():
            raise FileNotFoundError(f"CSV file not found: {file_path}")
        
        logger.info(f"Reading CSV file: {file_path}")
        
        try:
            # Get schema-based configuration
            dtypes = self.schema_reader.get_pandas_dtypes(table_name)
            column_names = self.schema_reader.get_column_names(table_name)
            
            # Read CSV with proper data types
            df = pd.read_csv(
                file_path,
                header=None,  # No header in the CSV files
                names=column_names,  # Use schema-defined column names
                dtype=dtypes,  # Use schema-defined data types
                na_values=['', 'NULL', 'null', 'None'],  # Handle missing values
                keep_default_na=True
            )
            
            logger.info(f"Successfully read {len(df)} rows from {table_name}")
            logger.debug(f"DataFrame info for {table_name}:")
            logger.debug(f"  Shape: {df.shape}")
            logger.debug(f"  Columns: {list(df.columns)}")
            logger.debug(f"  Data types: {dict(df.dtypes)}")
            
            return df
            
        except pd.errors.EmptyDataError:
            logger.error(f"CSV file is empty: {file_path}")
            raise
        except pd.errors.ParserError as e:
            logger.error(f"Error parsing CSV file {file_path}: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error reading CSV file {file_path}: {e}")
            raise
    
    def _get_table_file_path(self, table_name: str) -> str:
        """
        Get the file path for a table based on configuration.
        
        Args:
            table_name: Name of the table
            
        Returns:
            File path for the table's CSV file
        """
        base_path = self.config.get_setting('input.base_path', 'data/input/retail_db')
        file_pattern = self.config.get_setting('input.file_pattern', 'part-00000')
        
        file_path = Path(base_path) / table_name / file_pattern
        return str(file_path)
    
    def get_file_info(self, table_name: str) -> Dict[str, Any]:
        """
        Get information about a CSV file without reading all data.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Dictionary containing file information
        """
        file_path = Path(self._get_table_file_path(table_name))
        
        info = {
            'table_name': table_name,
            'file_path': str(file_path),
            'exists': file_path.exists(),
            'size_bytes': 0,
            'estimated_rows': 0
        }
        
        if file_path.exists():
            info['size_bytes'] = file_path.stat().st_size
            
            # Estimate number of rows by reading first few lines
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    lines = sum(1 for _ in f)
                info['estimated_rows'] = lines
            except Exception as e:
                logger.warning(f"Could not estimate rows for {table_name}: {e}")
                info['estimated_rows'] = 'unknown'
        
        logger.debug(f"File info for {table_name}: {info}")
        return info
    
    def validate_csv_structure(self, table_name: str) -> bool:
        """
        Validate that CSV file structure matches schema.
        
        Args:
            table_name: Name of the table to validate
            
        Returns:
            True if structure is valid
            
        Raises:
            ValueError: If validation fails
        """
        file_path = Path(self._get_table_file_path(table_name))
        
        if not file_path.exists():
            raise ValueError(f"CSV file not found: {file_path}")
        
        try:
            # Read just the first row to check structure
            df_sample = pd.read_csv(file_path, header=None, nrows=1)
            actual_columns = len(df_sample.columns)
            
            # Get expected columns from schema
            expected_columns = len(self.schema_reader.get_column_names(table_name))
            
            if actual_columns != expected_columns:
                raise ValueError(
                    f"Column count mismatch for {table_name}: "
                    f"expected {expected_columns}, found {actual_columns}"
                )
            
            logger.info(f"CSV structure validation passed for {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"CSV structure validation failed for {table_name}: {e}")
            raise
    
    def read_all_tables(self) -> Dict[str, pd.DataFrame]:
        """
        Read all available tables into DataFrames.
        
        Returns:
            Dictionary mapping table names to DataFrames
        """
        all_data = {}
        available_tables = self.schema_reader.get_available_tables()
        
        logger.info(f"Reading {len(available_tables)} tables")
        
        for table_name in available_tables:
            try:
                df = self.read_csv_file(table_name)
                all_data[table_name] = df
                logger.info(f"✅ Successfully read {table_name}: {len(df)} rows")
            except Exception as e:
                logger.error(f"❌ Failed to read {table_name}: {e}")
                # Continue with other tables
                continue
        
        logger.info(f"Successfully read {len(all_data)} out of {len(available_tables)} tables")
        return all_data


# Global instance for easy access
csv_reader = CSVReader() 