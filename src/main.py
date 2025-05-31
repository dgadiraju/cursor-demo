"""
Main orchestration pipeline for CSV to JSON conversion.

This module provides the main entry point and orchestrates the entire
conversion process from CSV files to JSON output.
"""

import sys
from typing import Dict, Any, Optional
from datetime import datetime
from pathlib import Path

# Import all components
from src.utils.config import config_manager
from src.utils.logger import get_logger, setup_logger
from src.readers.csv_reader import csv_reader
from src.processors.data_validator import data_validator
from src.processors.data_transformer import data_transformer
from src.writers.json_writer import json_writer

# Set up main logger
logger = setup_logger('main')


class CSVToJSONConverter:
    """Main converter class that orchestrates the entire conversion process."""
    
    def __init__(self):
        """Initialize the converter."""
        self.config = config_manager
        self.csv_reader = csv_reader
        self.data_validator = data_validator
        self.data_transformer = data_transformer
        self.json_writer = json_writer
        
        self.start_time = None
        self.end_time = None
        self.conversion_stats = {}
    
    def run_conversion(self, validate_data: Optional[bool] = None) -> Dict[str, Any]:
        """
        Run the complete CSV to JSON conversion process.
        
        Args:
            validate_data: Whether to validate data (overrides config setting)
            
        Returns:
            Dictionary containing conversion results and statistics
        """
        self.start_time = datetime.now()
        logger.info("=" * 80)
        logger.info("Starting CSV to JSON Conversion Process")
        logger.info("=" * 80)
        
        try:
            # Phase 1: Initialize and validate configuration
            logger.info("Phase 1: Initializing configuration...")
            self._initialize_config()
            
            # Phase 2: Read CSV data
            logger.info("Phase 2: Reading CSV files...")
            raw_data = self._read_csv_data()
            
            # Phase 3: Validate data (optional)
            should_validate = validate_data if validate_data is not None else self.config.get_setting('processing.validate_data', True)
            if should_validate:
                logger.info("Phase 3: Validating data...")
                validation_results = self._validate_data(raw_data)
            else:
                logger.info("Phase 3: Skipping data validation (disabled)")
                validation_results = {}
            
            # Phase 4: Transform data
            logger.info("Phase 4: Transforming data...")
            transformed_data = self._transform_data(raw_data)
            
            # Phase 5: Write JSON files
            logger.info("Phase 5: Writing JSON files...")
            write_results = self._write_json_files(transformed_data)
            
            # Compile final results
            self.end_time = datetime.now()
            results = self._compile_results(raw_data, validation_results, transformed_data, write_results)
            
            logger.info("=" * 80)
            logger.info("CSV to JSON Conversion Completed Successfully!")
            logger.info("=" * 80)
            
            return results
            
        except Exception as e:
            logger.error(f"Conversion process failed: {e}")
            raise
    
    def _initialize_config(self):
        """Initialize and validate configuration."""
        try:
            # Load configurations
            settings = self.config.load_settings()
            schemas = self.config.load_schemas()
            
            logger.info(f"Loaded settings: {len(settings)} sections")
            logger.info(f"Loaded schemas: {len(schemas.get('tables', {}))} tables")
            
            # Ensure output directory exists
            output_dir = Path(self.config.get_setting('output.base_path', 'data/output'))
            output_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"Output directory ready: {output_dir}")
            
        except Exception as e:
            logger.error(f"Configuration initialization failed: {e}")
            raise
    
    def _read_csv_data(self) -> Dict[str, Any]:
        """Read all CSV files."""
        try:
            data_dict = self.csv_reader.read_all_tables()
            
            if not data_dict:
                raise ValueError("No CSV data was successfully read")
            
            # Log summary
            total_rows = sum(len(df) for df in data_dict.values())
            logger.info(f"Successfully read {len(data_dict)} tables with {total_rows:,} total rows")
            
            return data_dict
            
        except Exception as e:
            logger.error(f"CSV reading phase failed: {e}")
            raise
    
    def _validate_data(self, data_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Validate all data."""
        try:
            validation_results = {}
            
            for table_name, df in data_dict.items():
                logger.info(f"Validating {table_name}...")
                result = self.data_validator.validate_dataframe(df, table_name)
                validation_results[table_name] = result
            
            # Print validation report
            self.data_validator.print_validation_report()
            
            return validation_results
            
        except Exception as e:
            logger.error(f"Data validation phase failed: {e}")
            raise
    
    def _transform_data(self, data_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Transform all data for JSON output."""
        try:
            transformed_data = self.data_transformer.transform_all_tables(data_dict)
            
            if not transformed_data:
                raise ValueError("No data was successfully transformed")
            
            # Log transformation summary
            summary = self.data_transformer.get_transformation_summary()
            logger.info(f"Transformation completed: {summary['tables_transformed']} tables, "
                       f"{summary['total_records']:,} records")
            
            return transformed_data
            
        except Exception as e:
            logger.error(f"Data transformation phase failed: {e}")
            raise
    
    def _write_json_files(self, transformed_data: Dict[str, Any]) -> Dict[str, Any]:
        """Write JSON files."""
        try:
            results = {}
            
            # Write individual table files
            individual_files = self.config.get_setting('output.individual_files', True)
            if individual_files:
                logger.info("Writing individual table JSON files...")
                individual_results = self.json_writer.write_all_tables(transformed_data)
                results['individual_files'] = individual_results
            
            # Write combined file
            combined_file = self.config.get_setting('output.combined_file', True)
            if combined_file:
                logger.info("Creating and writing combined dataset...")
                combined_data = self.data_transformer.create_combined_dataset(transformed_data)
                combined_path = self.json_writer.write_combined_json(combined_data)
                results['combined_file'] = combined_path
            
            # Print write report
            self.json_writer.print_write_report()
            
            # Validate output files
            logger.info("Validating output files...")
            validation_results = self.json_writer.validate_output_files()
            results['validation'] = validation_results
            
            return results
            
        except Exception as e:
            logger.error(f"JSON writing phase failed: {e}")
            raise
    
    def _compile_results(self, raw_data, validation_results, transformed_data, write_results) -> Dict[str, Any]:
        """Compile final conversion results."""
        duration = (self.end_time - self.start_time).total_seconds()
        
        results = {
            'success': True,
            'start_time': self.start_time.isoformat(),
            'end_time': self.end_time.isoformat(),
            'duration_seconds': duration,
            'summary': {
                'tables_processed': len(raw_data),
                'total_rows_read': sum(len(df) for df in raw_data.values()),
                'total_rows_transformed': sum(
                    len(data['data']) for data in transformed_data.values()
                ),
                'files_written': len(write_results.get('individual_files', {})),
                'combined_file_created': 'combined_file' in write_results
            },
            'phases': {
                'reading': {
                    'tables_read': len(raw_data),
                    'total_rows': sum(len(df) for df in raw_data.values())
                },
                'validation': {
                    'enabled': bool(validation_results),
                    'results': validation_results
                },
                'transformation': self.data_transformer.get_transformation_summary(),
                'writing': self.json_writer.get_write_summary()
            },
            'output_files': write_results
        }
        
        return results
    
    def print_final_report(self, results: Dict[str, Any]):
        """Print a comprehensive final report."""
        print("\n" + "=" * 80)
        print("CSV TO JSON CONVERSION - FINAL REPORT")
        print("=" * 80)
        
        summary = results['summary']
        print(f"Status: {'SUCCESS' if results['success'] else 'FAILED'}")
        print(f"Duration: {results['duration_seconds']:.2f} seconds")
        print(f"Tables Processed: {summary['tables_processed']}")
        print(f"Total Rows: {summary['total_rows_read']:,}")
        print(f"Files Written: {summary['files_written']}")
        print(f"Combined File: {'Yes' if summary['combined_file_created'] else 'No'}")
        
        # Output file locations
        print(f"\nOutput Directory: {self.config.get_setting('output.base_path', 'data/output')}")
        
        if 'individual_files' in results['output_files']:
            print("\nIndividual Files:")
            for table, path in results['output_files']['individual_files'].items():
                print(f"  • {table}: {path}")
        
        if 'combined_file' in results['output_files']:
            print(f"\nCombined File: {results['output_files']['combined_file']}")
        
        print("\n" + "=" * 80)


def main():
    """Main entry point for the conversion script."""
    try:
        converter = CSVToJSONConverter()
        results = converter.run_conversion()
        converter.print_final_report(results)
        
        return 0  # Success
        
    except KeyboardInterrupt:
        logger.info("Conversion interrupted by user")
        return 1
    except Exception as e:
        logger.error(f"Conversion failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main()) 