"""
Data validation utilities.

This module provides functionality to validate data integrity and completeness
during the CSV to JSON conversion process.
"""

from typing import Dict, Any, List, Tuple
import pandas as pd
from src.utils.logger import get_logger
from src.readers.schema_reader import schema_reader

logger = get_logger(__name__)


class DataValidator:
    """Handles validation of data integrity and completeness."""
    
    def __init__(self):
        """Initialize DataValidator."""
        self.schema_reader = schema_reader
        self.validation_results = {}
    
    def validate_dataframe(self, df: pd.DataFrame, table_name: str) -> Dict[str, Any]:
        """
        Validate a DataFrame against its schema.
        
        Args:
            df: DataFrame to validate
            table_name: Name of the table for schema reference
            
        Returns:
            Dictionary containing validation results
        """
        logger.info(f"Starting validation for table {table_name}")
        
        results = {
            'table_name': table_name,
            'total_rows': len(df),
            'valid_rows': 0,
            'issues': [],
            'warnings': [],
            'errors': [],
            'data_quality_score': 0.0
        }
        
        try:
            # 1. Validate required columns exist
            self._validate_required_columns(df, table_name, results)
            
            # 2. Validate data types
            self._validate_data_types(df, table_name, results)
            
            # 3. Validate required fields are not null
            self._validate_required_fields(df, table_name, results)
            
            # 4. Validate data ranges and formats
            self._validate_data_ranges(df, table_name, results)
            
            # 5. Calculate data quality score
            results['data_quality_score'] = self._calculate_quality_score(results)
            
            # 6. Determine valid rows count
            results['valid_rows'] = self._count_valid_rows(df, table_name, results)
            
            logger.info(f"Validation completed for {table_name}: "
                       f"Quality score: {results['data_quality_score']:.2f}")
            
        except Exception as e:
            logger.error(f"Validation failed for {table_name}: {e}")
            results['errors'].append(f"Validation process failed: {e}")
        
        self.validation_results[table_name] = results
        return results
    
    def _validate_required_columns(self, df: pd.DataFrame, table_name: str, results: Dict[str, Any]):
        """Validate that all required columns are present."""
        expected_columns = set(self.schema_reader.get_column_names(table_name))
        actual_columns = set(df.columns)
        
        missing_columns = expected_columns - actual_columns
        extra_columns = actual_columns - expected_columns
        
        if missing_columns:
            error_msg = f"Missing required columns: {list(missing_columns)}"
            results['errors'].append(error_msg)
            logger.error(f"{table_name}: {error_msg}")
        
        if extra_columns:
            warning_msg = f"Extra columns found: {list(extra_columns)}"
            results['warnings'].append(warning_msg)
            logger.warning(f"{table_name}: {warning_msg}")
    
    def _validate_data_types(self, df: pd.DataFrame, table_name: str, results: Dict[str, Any]):
        """Validate that data types match schema expectations."""
        expected_dtypes = self.schema_reader.get_pandas_dtypes(table_name)
        
        for col_name, expected_dtype in expected_dtypes.items():
            if col_name in df.columns:
                actual_dtype = str(df[col_name].dtype)
                
                # Check for type compatibility
                if not self._types_compatible(actual_dtype, expected_dtype):
                    issue_msg = f"Column {col_name}: expected {expected_dtype}, got {actual_dtype}"
                    results['issues'].append(issue_msg)
                    logger.warning(f"{table_name}: {issue_msg}")
    
    def _validate_required_fields(self, df: pd.DataFrame, table_name: str, results: Dict[str, Any]):
        """Validate that required fields are not null."""
        required_columns = self.schema_reader.get_required_columns(table_name)
        
        for col_name in required_columns:
            if col_name in df.columns:
                null_count = df[col_name].isnull().sum()
                if null_count > 0:
                    error_msg = f"Column {col_name}: {null_count} null values in required field"
                    results['errors'].append(error_msg)
                    logger.error(f"{table_name}: {error_msg}")
    
    def _validate_data_ranges(self, df: pd.DataFrame, table_name: str, results: Dict[str, Any]):
        """Validate data ranges and formats."""
        
        # Validate numeric columns for reasonable ranges
        numeric_columns = df.select_dtypes(include=['int64', 'Int64', 'float64']).columns
        
        for col_name in numeric_columns:
            if col_name.endswith('_id'):
                # ID fields should be positive
                negative_ids = (df[col_name] < 0).sum()
                if negative_ids > 0:
                    issue_msg = f"Column {col_name}: {negative_ids} negative ID values"
                    results['issues'].append(issue_msg)
                    logger.warning(f"{table_name}: {issue_msg}")
            
            elif col_name.endswith('_price') or col_name.endswith('_subtotal'):
                # Price fields should be non-negative
                negative_prices = (df[col_name] < 0).sum()
                if negative_prices > 0:
                    issue_msg = f"Column {col_name}: {negative_prices} negative price values"
                    results['issues'].append(issue_msg)
                    logger.warning(f"{table_name}: {issue_msg}")
        
        # Validate string columns for empty values
        string_columns = df.select_dtypes(include=['string', 'object']).columns
        
        for col_name in string_columns:
            empty_strings = (df[col_name] == '').sum()
            if empty_strings > 0:
                info_msg = f"Column {col_name}: {empty_strings} empty string values"
                results['warnings'].append(info_msg)
                logger.info(f"{table_name}: {info_msg}")
    
    def _types_compatible(self, actual_type: str, expected_type: str) -> bool:
        """Check if actual and expected types are compatible."""
        # Define type compatibility mapping
        compatibility = {
            'Int64': ['int64', 'Int64'],
            'int64': ['int64', 'Int64'],
            'float64': ['float64', 'float32'],
            'string': ['string', 'object']
        }
        
        expected_compatible = compatibility.get(expected_type, [expected_type])
        return any(actual_type.startswith(comp) for comp in expected_compatible)
    
    def _calculate_quality_score(self, results: Dict[str, Any]) -> float:
        """Calculate a data quality score based on validation results."""
        error_count = len(results['errors'])
        warning_count = len(results['warnings'])
        issue_count = len(results['issues'])
        
        # Base score starts at 100
        score = 100.0
        
        # Deduct points for issues
        score -= error_count * 10  # Errors are serious
        score -= warning_count * 5  # Warnings are moderate
        score -= issue_count * 2   # Issues are minor
        
        # Ensure score doesn't go below 0
        return max(0.0, score)
    
    def _count_valid_rows(self, df: pd.DataFrame, table_name: str, results: Dict[str, Any]) -> int:
        """Count rows that pass all validation checks."""
        if results['errors']:
            # If there are structural errors, we can't determine valid rows reliably
            return 0
        
        # Start with all rows as potentially valid
        valid_mask = pd.Series([True] * len(df))
        
        # Check required fields are not null
        required_columns = self.schema_reader.get_required_columns(table_name)
        for col_name in required_columns:
            if col_name in df.columns:
                valid_mask &= df[col_name].notna()
        
        return valid_mask.sum()
    
    def get_validation_summary(self) -> Dict[str, Any]:
        """
        Get a summary of all validation results.
        
        Returns:
            Dictionary containing validation summary
        """
        summary = {
            'tables_validated': len(self.validation_results),
            'overall_quality_score': 0.0,
            'total_rows': 0,
            'total_valid_rows': 0,
            'tables_with_errors': 0,
            'tables_with_warnings': 0
        }
        
        if not self.validation_results:
            return summary
        
        total_score = 0.0
        for table_name, results in self.validation_results.items():
            total_score += results['data_quality_score']
            summary['total_rows'] += results['total_rows']
            summary['total_valid_rows'] += results['valid_rows']
            
            if results['errors']:
                summary['tables_with_errors'] += 1
            if results['warnings']:
                summary['tables_with_warnings'] += 1
        
        summary['overall_quality_score'] = total_score / len(self.validation_results)
        
        return summary
    
    def print_validation_report(self):
        """Print a detailed validation report."""
        summary = self.get_validation_summary()
        
        print("\n" + "=" * 60)
        print("DATA VALIDATION REPORT")
        print("=" * 60)
        
        print(f"Tables Validated: {summary['tables_validated']}")
        print(f"Overall Quality Score: {summary['overall_quality_score']:.2f}/100")
        print(f"Total Rows: {summary['total_rows']:,}")
        print(f"Valid Rows: {summary['total_valid_rows']:,}")
        print(f"Tables with Errors: {summary['tables_with_errors']}")
        print(f"Tables with Warnings: {summary['tables_with_warnings']}")
        
        print("\nPer-Table Results:")
        print("-" * 60)
        
        for table_name, results in self.validation_results.items():
            print(f"\n{table_name.upper()}:")
            print(f"  Rows: {results['total_rows']:,} (Valid: {results['valid_rows']:,})")
            print(f"  Quality Score: {results['data_quality_score']:.2f}/100")
            
            if results['errors']:
                print(f"  ❌ Errors ({len(results['errors'])}):")
                for error in results['errors']:
                    print(f"    • {error}")
            
            if results['warnings']:
                print(f"  ⚠️  Warnings ({len(results['warnings'])}):")
                for warning in results['warnings']:
                    print(f"    • {warning}")
            
            if results['issues']:
                print(f"  ℹ️  Issues ({len(results['issues'])}):")
                for issue in results['issues']:
                    print(f"    • {issue}")


# Global instance for easy access
data_validator = DataValidator() 