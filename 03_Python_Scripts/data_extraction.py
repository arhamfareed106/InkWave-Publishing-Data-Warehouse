"""
InkWave Publishing Data Warehouse - CSV Data Extractor
Author: Data Engineering Team
Date: 2025-12-07
Purpose: Extract and validate data from CSV source files
"""

import pandas as pd
import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import os

from config.config import SOURCE_FILES, DATA_QUALITY_RULES, ETL_CONFIG

logger = logging.getLogger('inkwave.extractors.csv')


class CSVExtractor:
    """Extract and validate data from CSV files."""
    
    def __init__(self):
        """Initialize CSV extractor."""
        self.source_files = SOURCE_FILES
        self.data_quality_rules = DATA_QUALITY_RULES
        self.date_formats = ETL_CONFIG['date_formats']
    
    def extract_raw_daily(self) -> Tuple[pd.DataFrame, Dict]:
        """
        Extract data from raw_daily.csv.
        
        Returns:
            Tuple of (DataFrame, metadata dict)
        """
        logger.info("Extracting raw_daily.csv")
        
        file_path = self.source_files['raw_daily']
        
        try:
            df = pd.read_csv(file_path, encoding='utf-8')
            
            # Basic validation
            rules = self.data_quality_rules['raw_daily']
            missing_cols = set(rules['required_columns']) - set(df.columns)
            
            if missing_cols:
                logger.error(f"Missing required columns: {missing_cols}")
                raise ValueError(f"Missing columns: {missing_cols}")
            
            metadata = {
                'file_name': os.path.basename(file_path),
                'total_rows': len(df),
                'columns': list(df.columns),
                'extract_timestamp': datetime.now(),
                'file_size_bytes': os.path.getsize(file_path)
            }
            
            logger.info(f"Extracted {len(df)} rows from raw_daily.csv")
            return df, metadata
            
        except Exception as e:
            logger.error(f"Failed to extract raw_daily.csv: {str(e)}")
            raise
    
    def extract_raw_sales(self) -> Tuple[pd.DataFrame, Dict]:
        """
        Extract data from raw_sales.csv.
        
        Returns:
            Tuple of (DataFrame, metadata dict)
        """
        logger.info("Extracting raw_sales.csv")
        
        file_path = self.source_files['raw_sales']
        
        try:
            df = pd.read_csv(file_path, encoding='utf-8')
            
            # Basic validation
            rules = self.data_quality_rules['raw_sales']
            missing_cols = set(rules['required_columns']) - set(df.columns)
            
            if missing_cols:
                logger.error(f"Missing required columns: {missing_cols}")
                raise ValueError(f"Missing columns: {missing_cols}")
            
            metadata = {
                'file_name': os.path.basename(file_path),
                'total_rows': len(df),
                'columns': list(df.columns),
                'extract_timestamp': datetime.now(),
                'file_size_bytes': os.path.getsize(file_path)
            }
            
            logger.info(f"Extracted {len(df)} rows from raw_sales.csv")
            return df, metadata
            
        except Exception as e:
            logger.error(f"Failed to extract raw_sales.csv: {str(e)}")
            raise
    
    def extract_raw_meta(self) -> Tuple[pd.DataFrame, Dict]:
        """
        Extract data from raw_meta.csv.
        
        Returns:
            Tuple of (DataFrame, metadata dict)
        """
        logger.info("Extracting raw_meta.csv")
        
        file_path = self.source_files['raw_meta']
        
        try:
            df = pd.read_csv(file_path, encoding='utf-8')
            
            metadata = {
                'file_name': os.path.basename(file_path),
                'total_rows': len(df),
                'columns': list(df.columns),
                'extract_timestamp': datetime.now(),
                'file_size_bytes': os.path.getsize(file_path)
            }
            
            logger.info(f"Extracted {len(df)} rows from raw_meta.csv")
            return df, metadata
            
        except Exception as e:
            logger.error(f"Failed to extract raw_meta.csv: {str(e)}")
            raise
    
    def extract_all_sources(self) -> Dict[str, Tuple[pd.DataFrame, Dict]]:
        """
        Extract all CSV source files.
        
        Returns:
            Dictionary mapping source name to (DataFrame, metadata) tuple
        """
        logger.info("Starting extraction of all CSV sources")
        
        sources = {}
        
        try:
            sources['raw_daily'] = self.extract_raw_daily()
            sources['raw_sales'] = self.extract_raw_sales()
            sources['raw_meta'] = self.extract_raw_meta()
            
            total_rows = sum(df.shape[0] for df, _ in sources.values())
            logger.info(f"Total rows extracted: {total_rows}")
            
            return sources
            
        except Exception as e:
            logger.error(f"Failed to extract all sources: {str(e)}")
            raise
    
    def parse_date_column(self, date_series: pd.Series) -> Tuple[pd.Series, pd.Series]:
        """
        Parse date column with multiple format support.
        
        Args:
            date_series: Pandas Series containing date strings
            
        Returns:
            Tuple of (parsed dates Series, format detected Series)
        """
        parsed_dates = pd.Series([None] * len(date_series))
        formats_detected = pd.Series([''] * len(date_series))
        
        for idx, date_str in enumerate(date_series):
            if pd.isna(date_str):
                continue
            
            date_str = str(date_str).strip()
            
            for date_format in self.date_formats:
                try:
                    parsed_date = datetime.strptime(date_str, date_format)
                    parsed_dates[idx] = parsed_date
                    formats_detected[idx] = date_format
                    break
                except ValueError:
                    continue
            
            if parsed_dates[idx] is None:
                logger.warning(f"Could not parse date: {date_str}")
        
        return parsed_dates, formats_detected
    
    def validate_data_quality(self, df: pd.DataFrame, source_name: str) -> Dict:
        """
        Validate data quality against rules.
        
        Args:
            df: DataFrame to validate
            source_name: Name of source (raw_daily, raw_sales, etc.)
            
        Returns:
            Dictionary with validation results
        """
        logger.info(f"Validating data quality for {source_name}")
        
        results = {
            'source': source_name,
            'total_rows': len(df),
            'issues': [],
            'warnings': [],
            'errors': []
        }
        
        if source_name not in self.data_quality_rules:
            logger.warning(f"No validation rules defined for {source_name}")
            return results
        
        rules = self.data_quality_rules[source_name]
        
        # Check numeric columns
        if 'numeric_columns' in rules:
            for col in rules['numeric_columns']:
                if col in df.columns:
                    non_numeric = df[~pd.to_numeric(df[col], errors='coerce').notna()][col]
                    if len(non_numeric) > 0:
                        results['warnings'].append(
                            f"{col}: {len(non_numeric)} non-numeric values"
                        )
        
        # Check positive value constraints
        if 'positive_columns' in rules:
            for col in rules['positive_columns']:
                if col in df.columns:
                    negative_vals = df[df[col] < 0]
                    if len(negative_vals) > 0:
                        results['errors'].append(
                            f"{col}: {len(negative_vals)} negative values (should be positive)"
                        )
        
        # Check range constraints
        if 'range_checks' in rules:
            for col, (min_val, max_val) in rules['range_checks'].items():
                if col in df.columns:
                    out_of_range = df[(df[col] < min_val) | (df[col] > max_val)]
                    if len(out_of_range) > 0:
                        results['warnings'].append(
                            f"{col}: {len(out_of_range)} values out of range [{min_val}, {max_val}]"
                        )
        
        # Check categorical values
        if 'categorical_checks' in rules:
            for col, valid_values in rules['categorical_checks'].items():
                if col in df.columns:
                    invalid_vals = df[~df[col].isin(valid_values)]
                    if len(invalid_vals) > 0:
                        results['errors'].append(
                            f"{col}: {len(invalid_vals)} invalid values (expected: {valid_values})"
                        )
        
        # Summary
        results['issues'] = results['warnings'] + results['errors']
        logger.info(f"Validation complete: {len(results['warnings'])} warnings, {len(results['errors'])} errors")
        
        return results


if __name__ == "__main__":
    # Test extractor
    logging.basicConfig(level=logging.INFO)
    
    print("Testing CSV Extractor")
    print("=" * 60)
    
    extractor = CSVExtractor()
    
    try:
        sources = extractor.extract_all_sources()
        
        for source_name, (df, metadata) in sources.items():
            print(f"\n{source_name}:")
            print(f"  Rows: {metadata['total_rows']}")
            print(f"  Columns: {len(metadata['columns'])}")
            print(f"  File size: {metadata['file_size_bytes']} bytes")
            
            # Validate
            validation = extractor.validate_data_quality(df, source_name)
            print(f"  Warnings: {len(validation['warnings'])}")
            print(f"  Errors: {len(validation['errors'])}")
            
            # Show first few rows
            print(f"\n  Sample data:")
            print(df.head(3).to_string(index=False))
        
        print("\n✓ Extraction complete")
        
    except Exception as e:
        print(f"\n✗ Extraction failed: {str(e)}")
