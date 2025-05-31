"""
Schema reading and parsing utilities.

This module provides functionality to load and parse schema definitions
from the configuration files.
"""

from typing import Dict, Any, List
import pandas as pd
from src.utils.config import config_manager
from src.utils.logger import get_logger

logger = get_logger(__name__)


class SchemaReader:
    """Handles loading and parsing of data schemas."""
    
    def __init__(self):
        """Initialize SchemaReader."""
        self.config = config_manager
        self._table_schemas = None
    
    def load_all_schemas(self) -> Dict[str, Any]:
        """
        Load all table schemas from configuration.
        
        Returns:
            Dictionary containing all table schemas
        """
        if self._table_schemas is None:
            try:
                schemas = self.config.load_schemas()
                self._table_schemas = schemas.get('tables', {})
                logger.info(f"Loaded schemas for {len(self._table_schemas)} tables")
            except Exception as e:
                logger.error(f"Failed to load schemas: {e}")
                raise
                
        return self._table_schemas
    
    def get_table_schema(self, table_name: str) -> Dict[str, Any]:
        """
        Get schema for a specific table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Table schema configuration
            
        Raises:
            KeyError: If table schema not found
        """
        schemas = self.load_all_schemas()
        
        if table_name not in schemas:
            raise KeyError(f"Schema not found for table: {table_name}")
            
        return schemas[table_name]
    
    def get_pandas_dtypes(self, table_name: str) -> Dict[str, str]:
        """
        Get pandas-compatible data types for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Dictionary mapping column names to pandas dtypes
        """
        schema = self.get_table_schema(table_name)
        columns = schema.get('columns', {})
        
        dtypes = {}
        for col_name, col_config in columns.items():
            col_type = col_config.get('type', 'string')
            
            # Map schema types to pandas types
            if col_type == 'int64':
                dtypes[col_name] = 'Int64'  # Nullable integer
            elif col_type == 'float64':
                dtypes[col_name] = 'float64'
            elif col_type == 'string':
                dtypes[col_name] = 'string'
            else:
                logger.warning(f"Unknown type '{col_type}' for {col_name}, using string")
                dtypes[col_name] = 'string'
        
        logger.debug(f"Generated dtypes for {table_name}: {dtypes}")
        return dtypes
    
    def get_column_names(self, table_name: str) -> List[str]:
        """
        Get ordered list of column names for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            List of column names in position order
        """
        schema = self.get_table_schema(table_name)
        columns = schema.get('columns', {})
        
        # Sort by position
        sorted_columns = sorted(
            columns.items(),
            key=lambda x: x[1].get('position', 0)
        )
        
        column_names = [col_name for col_name, _ in sorted_columns]
        logger.debug(f"Column order for {table_name}: {column_names}")
        return column_names
    
    def get_required_columns(self, table_name: str) -> List[str]:
        """
        Get list of required columns for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            List of required column names
        """
        schema = self.get_table_schema(table_name)
        columns = schema.get('columns', {})
        
        required_cols = [
            col_name for col_name, col_config in columns.items()
            if col_config.get('required', False)
        ]
        
        logger.debug(f"Required columns for {table_name}: {required_cols}")
        return required_cols
    
    def validate_schema(self, table_name: str) -> bool:
        """
        Validate that a table schema is properly configured.
        
        Args:
            table_name: Name of the table to validate
            
        Returns:
            True if schema is valid
            
        Raises:
            ValueError: If schema validation fails
        """
        try:
            schema = self.get_table_schema(table_name)
            
            if 'columns' not in schema:
                raise ValueError(f"No columns defined for table {table_name}")
            
            columns = schema['columns']
            if not columns:
                raise ValueError(f"Empty columns configuration for table {table_name}")
            
            # Check that all columns have required fields
            for col_name, col_config in columns.items():
                if 'type' not in col_config:
                    raise ValueError(f"Missing type for column {col_name} in table {table_name}")
                if 'position' not in col_config:
                    raise ValueError(f"Missing position for column {col_name} in table {table_name}")
            
            logger.info(f"Schema validation passed for table {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Schema validation failed for table {table_name}: {e}")
            raise
    
    def get_available_tables(self) -> List[str]:
        """
        Get list of all available table names.
        
        Returns:
            List of table names
        """
        return list(self.load_all_schemas().keys())


# Global instance for easy access
schema_reader = SchemaReader() 