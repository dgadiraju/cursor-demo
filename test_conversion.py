#!/usr/bin/env python3
"""
Test script for CSV to JSON conversion.

This script runs a quick test of the conversion pipeline to verify
all components work together correctly.
"""

import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from src.main import CSVToJSONConverter


def main():
    """Run conversion test."""
    print("Testing CSV to JSON Conversion Pipeline...")
    print("=" * 50)
    
    try:
        # Create converter instance
        converter = CSVToJSONConverter()
        
        # Run conversion
        results = converter.run_conversion()
        
        # Print results
        converter.print_final_report(results)
        
        print("\n✅ Test completed successfully!")
        return 0
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main()) 