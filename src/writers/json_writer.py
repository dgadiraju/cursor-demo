"""
JSON writing utilities.

This module provides functionality to write transformed data to JSON files
with proper formatting and error handling.
"""

from typing import Dict, Any, Optional
import json
from pathlib import Path
from datetime import datetime
from src.utils.config import config_manager
from src.utils.logger import get_logger

logger = get_logger(__name__)


class JSONWriter:
    """Handles writing data to JSON files."""
    
    def __init__(self):
        """Initialize JSONWriter."""
        self.config = config_manager
        self.write_stats = {}
    
    def write_table_json(self, data: Dict[str, Any], table_name: str, output_path: Optional[str] = None) -> str:
        """
        Write table data to a JSON file.
        
        Args:
            data: Transformed table data
            table_name: Name of the table
            output_path: Optional custom output path
            
        Returns:
            Path to the written file
            
        Raises:
            OSError: If file writing fails
        """
        if output_path is None:
            output_path = self._get_table_output_path(table_name)
        
        output_path = Path(output_path)
        
        # Ensure output directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Writing {table_name} to {output_path}")
        
        try:
            start_time = datetime.now()
            
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False, default=self._json_serializer)
            
            end_time = datetime.now()
            write_duration = (end_time - start_time).total_seconds()
            
            # Track write statistics
            file_size = output_path.stat().st_size
            record_count = len(data.get('data', []))
            
            self.write_stats[table_name] = {
                'file_path': str(output_path),
                'file_size_bytes': file_size,
                'record_count': record_count,
                'write_duration_seconds': write_duration,
                'write_time': end_time.isoformat()
            }
            
            logger.info(f"Successfully wrote {table_name}: {record_count} records, "
                       f"{file_size:,} bytes, {write_duration:.2f}s")
            
            return str(output_path)
            
        except OSError as e:
            logger.error(f"Failed to write {table_name} to {output_path}: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error writing {table_name}: {e}")
            raise
    
    def write_combined_json(self, combined_data: Dict[str, Any], output_path: Optional[str] = None) -> str:
        """
        Write combined dataset to a JSON file.
        
        Args:
            combined_data: Combined dataset
            output_path: Optional custom output path
            
        Returns:
            Path to the written file
        """
        if output_path is None:
            output_path = self._get_combined_output_path()
        
        output_path = Path(output_path)
        
        # Ensure output directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Writing combined dataset to {output_path}")
        
        try:
            start_time = datetime.now()
            
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(combined_data, f, indent=2, ensure_ascii=False, default=self._json_serializer)
            
            end_time = datetime.now()
            write_duration = (end_time - start_time).total_seconds()
            
            # Track write statistics
            file_size = output_path.stat().st_size
            total_records = combined_data.get('metadata', {}).get('total_records', 0)
            
            self.write_stats['_combined'] = {
                'file_path': str(output_path),
                'file_size_bytes': file_size,
                'record_count': total_records,
                'write_duration_seconds': write_duration,
                'write_time': end_time.isoformat()
            }
            
            logger.info(f"Successfully wrote combined dataset: {total_records} records, "
                       f"{file_size:,} bytes, {write_duration:.2f}s")
            
            return str(output_path)
            
        except OSError as e:
            logger.error(f"Failed to write combined dataset to {output_path}: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error writing combined dataset: {e}")
            raise
    
    def write_all_tables(self, transformed_data: Dict[str, Dict[str, Any]]) -> Dict[str, str]:
        """
        Write all table data to individual JSON files.
        
        Args:
            transformed_data: Dictionary of transformed table data
            
        Returns:
            Dictionary mapping table names to output file paths
        """
        written_files = {}
        
        logger.info(f"Writing {len(transformed_data)} tables to JSON files")
        
        for table_name, table_data in transformed_data.items():
            try:
                file_path = self.write_table_json(table_data, table_name)
                written_files[table_name] = file_path
                logger.info(f"✅ Successfully wrote {table_name}")
            except Exception as e:
                logger.error(f"❌ Failed to write {table_name}: {e}")
                # Continue with other tables
                continue
        
        logger.info(f"Successfully wrote {len(written_files)} out of {len(transformed_data)} tables")
        return written_files
    
    def _get_table_output_path(self, table_name: str) -> str:
        """
        Get output path for a table JSON file.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Output file path
        """
        base_path = self.config.get_setting('output.base_path', 'data/output')
        return str(Path(base_path) / f"{table_name}.json")
    
    def _get_combined_output_path(self) -> str:
        """
        Get output path for the combined JSON file.
        
        Returns:
            Output file path for combined data
        """
        base_path = self.config.get_setting('output.base_path', 'data/output')
        filename = self.config.get_setting('output.combined_filename', 'retail_db_combined.json')
        return str(Path(base_path) / filename)
    
    def _json_serializer(self, obj):
        """
        Custom JSON serializer for special data types.
        
        Args:
            obj: Object to serialize
            
        Returns:
            JSON-serializable representation
        """
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif hasattr(obj, 'item'):  # numpy/pandas scalars
            return obj.item()
        elif str(type(obj)).startswith('<class \'pandas'):
            # Handle any remaining pandas types
            return str(obj)
        
        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
    
    def get_write_summary(self) -> Dict[str, Any]:
        """
        Get summary of write operations.
        
        Returns:
            Dictionary containing write summary
        """
        if not self.write_stats:
            return {'files_written': 0, 'total_size_bytes': 0, 'total_records': 0}
        
        summary = {
            'files_written': len(self.write_stats),
            'total_size_bytes': sum(stats['file_size_bytes'] for stats in self.write_stats.values()),
            'total_records': sum(stats['record_count'] for stats in self.write_stats.values()),
            'total_duration_seconds': sum(stats['write_duration_seconds'] for stats in self.write_stats.values()),
            'files': {}
        }
        
        for file_name, stats in self.write_stats.items():
            summary['files'][file_name] = {
                'size_bytes': stats['file_size_bytes'],
                'records': stats['record_count'],
                'duration_seconds': stats['write_duration_seconds'],
                'path': stats['file_path']
            }
        
        return summary
    
    def print_write_report(self):
        """Print a detailed write operations report."""
        summary = self.get_write_summary()
        
        print("\n" + "=" * 60)
        print("JSON WRITE REPORT")
        print("=" * 60)
        
        print(f"Files Written: {summary['files_written']}")
        print(f"Total Size: {summary['total_size_bytes']:,} bytes ({summary['total_size_bytes']/1024/1024:.2f} MB)")
        print(f"Total Records: {summary['total_records']:,}")
        print(f"Total Duration: {summary['total_duration_seconds']:.2f} seconds")
        
        print("\nPer-File Results:")
        print("-" * 60)
        
        for file_name, stats in summary['files'].items():
            size_mb = stats['size_bytes'] / 1024 / 1024
            print(f"\n{file_name}:")
            print(f"  Path: {stats['path']}")
            print(f"  Records: {stats['records']:,}")
            print(f"  Size: {stats['size_bytes']:,} bytes ({size_mb:.2f} MB)")
            print(f"  Duration: {stats['duration_seconds']:.2f} seconds")
    
    def validate_output_files(self) -> Dict[str, bool]:
        """
        Validate that output files were created and are readable.
        
        Returns:
            Dictionary mapping file names to validation status
        """
        validation_results = {}
        
        for file_name, stats in self.write_stats.items():
            file_path = Path(stats['file_path'])
            
            try:
                # Check if file exists and is readable
                if not file_path.exists():
                    validation_results[file_name] = False
                    logger.error(f"Output file not found: {file_path}")
                    continue
                
                # Try to read and parse JSON
                with open(file_path, 'r', encoding='utf-8') as f:
                    json.load(f)
                
                validation_results[file_name] = True
                logger.debug(f"Output file validation passed: {file_path}")
                
            except Exception as e:
                validation_results[file_name] = False
                logger.error(f"Output file validation failed for {file_path}: {e}")
        
        return validation_results


# Global instance for easy access
json_writer = JSONWriter() 