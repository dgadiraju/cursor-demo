"""
Data transformation utilities.

This module provides functionality to transform and prepare data
for JSON output during the CSV to JSON conversion process.
"""

from typing import Dict, Any, List, Optional
import pandas as pd
import json
from datetime import datetime
from src.utils.logger import get_logger
from src.readers.schema_reader import schema_reader

logger = get_logger(__name__)


class DataTransformer:
    """Handles data transformation and preparation for JSON output."""
    
    def __init__(self):
        """Initialize DataTransformer."""
        self.schema_reader = schema_reader
        self.transformation_stats = {}
    
    def transform_dataframe(self, df: pd.DataFrame, table_name: str) -> Dict[str, Any]:
        """
        Transform DataFrame to JSON-ready format.
        
        Args:
            df: DataFrame to transform
            table_name: Name of the table for context
            
        Returns:
            Dictionary containing transformed data ready for JSON output
        """
        logger.info(f"Transforming {len(df)} rows for table {table_name}")
        
        try:
            # Clean and prepare data
            df_clean = self._clean_dataframe(df, table_name)
            
            # Convert to records format
            records = self._convert_to_records(df_clean, table_name)
            
            # Add metadata
            metadata = self._generate_metadata(df_clean, table_name)
            
            # Prepare final output structure
            result = {
                'metadata': metadata,
                'data': records
            }
            
            # Track transformation stats
            self.transformation_stats[table_name] = {
                'original_rows': len(df),
                'transformed_rows': len(records),
                'columns': list(df.columns),
                'transformation_time': datetime.now().isoformat()
            }
            
            logger.info(f"Successfully transformed {table_name}: {len(records)} records")
            return result
            
        except Exception as e:
            logger.error(f"Failed to transform {table_name}: {e}")
            raise
    
    def _clean_dataframe(self, df: pd.DataFrame, table_name: str) -> pd.DataFrame:
        """
        Clean DataFrame by handling missing values and data type issues.
        
        Args:
            df: DataFrame to clean
            table_name: Name of the table for schema reference
            
        Returns:
            Cleaned DataFrame
        """
        df_clean = df.copy()
        
        # Handle nullable integers (convert NaN to None for JSON)
        int_columns = df_clean.select_dtypes(include=['Int64']).columns
        for col in int_columns:
            # Convert pandas NA to None for JSON serialization
            df_clean[col] = df_clean[col].where(df_clean[col].notna(), None)
        
        # Handle string columns (convert NaN to None)
        string_columns = df_clean.select_dtypes(include=['string', 'object']).columns
        for col in string_columns:
            df_clean[col] = df_clean[col].where(df_clean[col].notna(), None)
            # Also convert empty strings to None if appropriate
            df_clean[col] = df_clean[col].where(df_clean[col] != '', None)
        
        # Handle float columns (keep NaN as None for JSON)
        float_columns = df_clean.select_dtypes(include=['float64']).columns
        for col in float_columns:
            df_clean[col] = df_clean[col].where(df_clean[col].notna(), None)
        
        logger.debug(f"Cleaned {table_name}: handled nulls in {len(df_clean.columns)} columns")
        return df_clean
    
    def _convert_to_records(self, df: pd.DataFrame, table_name: str) -> List[Dict[str, Any]]:
        """
        Convert DataFrame to list of record dictionaries.
        
        Args:
            df: Cleaned DataFrame
            table_name: Name of the table
            
        Returns:
            List of record dictionaries
        """
        # Convert to records, handling pandas-specific types
        records = []
        
        for _, row in df.iterrows():
            record = {}
            for col_name, value in row.items():
                # Handle pandas-specific types for JSON serialization
                if pd.isna(value):
                    record[col_name] = None
                elif isinstance(value, (pd.Int64Dtype, pd.StringDtype)):
                    record[col_name] = None if pd.isna(value) else value
                else:
                    # Convert pandas types to native Python types
                    if hasattr(value, 'item'):  # numpy/pandas scalars
                        record[col_name] = value.item()
                    else:
                        record[col_name] = value
            
            records.append(record)
        
        logger.debug(f"Converted {table_name} to {len(records)} records")
        return records
    
    def _generate_metadata(self, df: pd.DataFrame, table_name: str) -> Dict[str, Any]:
        """
        Generate metadata for the transformed data.
        
        Args:
            df: DataFrame
            table_name: Name of the table
            
        Returns:
            Metadata dictionary
        """
        # Calculate basic statistics
        metadata = {
            'table_name': table_name,
            'record_count': len(df),
            'column_count': len(df.columns),
            'columns': list(df.columns),
            'generated_at': datetime.now().isoformat(),
            'data_types': {},
            'statistics': {}
        }
        
        # Add data type information
        for col in df.columns:
            metadata['data_types'][col] = str(df[col].dtype)
        
        # Add basic statistics for numeric columns
        numeric_columns = df.select_dtypes(include=['int64', 'Int64', 'float64']).columns
        for col in numeric_columns:
            if not df[col].isna().all():  # Only if column has data
                metadata['statistics'][col] = {
                    'min': df[col].min() if not pd.isna(df[col].min()) else None,
                    'max': df[col].max() if not pd.isna(df[col].max()) else None,
                    'mean': df[col].mean() if not pd.isna(df[col].mean()) else None,
                    'null_count': int(df[col].isna().sum())
                }
        
        # Add null count for all columns
        for col in df.columns:
            if col not in metadata['statistics']:
                metadata['statistics'][col] = {}
            metadata['statistics'][col]['null_count'] = int(df[col].isna().sum())
        
        return metadata
    
    def transform_all_tables(self, data_dict: Dict[str, pd.DataFrame]) -> Dict[str, Dict[str, Any]]:
        """
        Transform all tables in the data dictionary.
        
        Args:
            data_dict: Dictionary mapping table names to DataFrames
            
        Returns:
            Dictionary mapping table names to transformed JSON-ready data
        """
        transformed_data = {}
        
        logger.info(f"Transforming {len(data_dict)} tables")
        
        for table_name, df in data_dict.items():
            try:
                transformed_data[table_name] = self.transform_dataframe(df, table_name)
                logger.info(f"✅ Successfully transformed {table_name}")
            except Exception as e:
                logger.error(f"❌ Failed to transform {table_name}: {e}")
                # Continue with other tables
                continue
        
        logger.info(f"Successfully transformed {len(transformed_data)} out of {len(data_dict)} tables")
        return transformed_data
    
    def get_transformation_summary(self) -> Dict[str, Any]:
        """
        Get summary of transformation statistics.
        
        Returns:
            Dictionary containing transformation summary
        """
        if not self.transformation_stats:
            return {'tables_transformed': 0, 'total_records': 0}
        
        summary = {
            'tables_transformed': len(self.transformation_stats),
            'total_records': sum(stats['transformed_rows'] for stats in self.transformation_stats.values()),
            'total_original_records': sum(stats['original_rows'] for stats in self.transformation_stats.values()),
            'tables': {}
        }
        
        for table_name, stats in self.transformation_stats.items():
            summary['tables'][table_name] = {
                'records': stats['transformed_rows'],
                'columns': len(stats['columns']),
                'transformation_time': stats['transformation_time']
            }
        
        return summary
    
    def create_combined_dataset(self, transformed_data: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """
        Create a combined dataset from all transformed tables.
        
        Args:
            transformed_data: Dictionary of transformed table data
            
        Returns:
            Combined dataset structure
        """
        logger.info("Creating combined dataset")
        
        combined = {
            'metadata': {
                'dataset_name': 'retail_db_combined',
                'created_at': datetime.now().isoformat(),
                'table_count': len(transformed_data),
                'total_records': 0,
                'tables': {}
            },
            'tables': {}
        }
        
        # Combine all table data
        for table_name, table_data in transformed_data.items():
            combined['tables'][table_name] = table_data['data']
            
            # Update metadata
            record_count = len(table_data['data'])
            combined['metadata']['total_records'] += record_count
            combined['metadata']['tables'][table_name] = {
                'record_count': record_count,
                'columns': table_data['metadata']['columns']
            }
        
        logger.info(f"Combined dataset created with {combined['metadata']['total_records']} total records")
        return combined


# Global instance for easy access
data_transformer = DataTransformer() 