"""
Configuration management utilities.

This module provides centralized configuration loading and management
for the CSV to JSON conversion project.
"""

import yaml
from pathlib import Path
from typing import Dict, Any
import logging


class ConfigManager:
    """Manages application configuration loading and access."""
    
    def __init__(self, config_dir: str = "config"):
        """
        Initialize ConfigManager.
        
        Args:
            config_dir: Directory containing configuration files
        """
        self.config_dir = Path(config_dir)
        self._settings = None
        self._schemas = None
        
    def load_settings(self) -> Dict[str, Any]:
        """
        Load application settings from settings.yaml.
        
        Returns:
            Dictionary containing application settings
            
        Raises:
            FileNotFoundError: If settings.yaml is not found
            yaml.YAMLError: If YAML parsing fails
        """
        if self._settings is None:
            settings_file = self.config_dir / "settings.yaml"
            
            if not settings_file.exists():
                raise FileNotFoundError(f"Settings file not found: {settings_file}")
                
            try:
                with open(settings_file, 'r', encoding='utf-8') as file:
                    self._settings = yaml.safe_load(file)
                    
                logging.info(f"Settings loaded from {settings_file}")
                    
            except yaml.YAMLError as e:
                logging.error(f"Error parsing settings YAML: {e}")
                raise
                
        return self._settings
    
    def load_schemas(self) -> Dict[str, Any]:
        """
        Load data schemas from schemas.yaml.
        
        Returns:
            Dictionary containing table schemas
            
        Raises:
            FileNotFoundError: If schemas.yaml is not found
            yaml.YAMLError: If YAML parsing fails
        """
        if self._schemas is None:
            schemas_file = self.config_dir / "schemas.yaml"
            
            if not schemas_file.exists():
                raise FileNotFoundError(f"Schemas file not found: {schemas_file}")
                
            try:
                with open(schemas_file, 'r', encoding='utf-8') as file:
                    self._schemas = yaml.safe_load(file)
                    
                logging.info(f"Schemas loaded from {schemas_file}")
                    
            except yaml.YAMLError as e:
                logging.error(f"Error parsing schemas YAML: {e}")
                raise
                
        return self._schemas
    
    def get_setting(self, key_path: str, default: Any = None) -> Any:
        """
        Get a specific setting using dot notation.
        
        Args:
            key_path: Dot-separated path to setting (e.g., 'input.base_path')
            default: Default value if setting not found
            
        Returns:
            Setting value or default
            
        Example:
            config.get_setting('input.base_path') -> 'data/input/retail_db'
        """
        settings = self.load_settings()
        keys = key_path.split('.')
        
        current = settings
        for key in keys:
            if isinstance(current, dict) and key in current:
                current = current[key]
            else:
                return default
                
        return current
    
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
        schemas = self.load_schemas()
        
        if 'tables' not in schemas:
            raise KeyError("No tables configuration found in schemas")
            
        if table_name not in schemas['tables']:
            raise KeyError(f"Schema not found for table: {table_name}")
            
        return schemas['tables'][table_name]
    
    def get_table_names(self) -> list:
        """
        Get list of all configured table names.
        
        Returns:
            List of table names
        """
        schemas = self.load_schemas()
        return list(schemas.get('tables', {}).keys())


# Global instance for easy access
config_manager = ConfigManager() 