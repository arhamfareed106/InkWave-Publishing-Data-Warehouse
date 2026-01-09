#!/usr/bin/env python3
"""
InkWave Publishing Data Warehouse - Main ETL Orchestration Script
Author: Data Engineering Team
Date: 2025-12-08
Purpose: Coordinate the complete ETL process for the data warehouse
"""

import logging
import sys
import os
import traceback

# Add the current directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config.config import SOURCE_FILES, ETL_CONFIG
from csv_extractor import CSVExtractor
from oracle_connector import get_connection_pool, close_connection_pool

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_process.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger('inkwave.etl.main')


def extract_data():
    """
    Extract data from all CSV sources
    """
    logger.info("Starting data extraction process")
    
    extractor = CSVExtractor()
    
    try:
        # Extract all sources
        sources = extractor.extract_all_sources()
        
        # Validate data quality for each source
        for source_name, (df, metadata) in sources.items():
            logger.info(f"Validating data quality for {source_name}")
            validation_results = extractor.validate_data_quality(df, source_name)
            
            if validation_results['errors']:
                logger.warning(f"Data quality errors found in {source_name}:")
                for error in validation_results['errors']:
                    logger.warning(f"  - {error}")
            
            if validation_results['warnings']:
                logger.info(f"Data quality warnings for {source_name}:")
                for warning in validation_results['warnings']:
                    logger.info(f"  - {warning}")
        
        logger.info("Data extraction completed successfully")
        return sources
        
    except Exception as e:
        logger.error(f"Data extraction failed: {str(e)}")
        logger.error(traceback.format_exc())
        raise


def stage_data(sources):
    """
    Stage extracted data in the database
    """
    logger.info("Starting data staging process")
    
    pool = get_connection_pool()
    
    try:
        # Process each source
        for source_name, (df, metadata) in sources.items():
            logger.info(f"Staging data from {source_name}")
            
            # Convert DataFrame to list of tuples for database insertion
            data_tuples = [tuple(row) for row in df.values]
            
            # Determine target table based on source
            if source_name == 'raw_daily':
                table_name = 'STG_RAW_DAILY'
                columns = ', '.join(df.columns)
            elif source_name == 'raw_sales':
                table_name = 'STG_RAW_SALES'
                columns = ', '.join(df.columns)
            elif source_name == 'raw_meta':
                table_name = 'STG_RAW_META'
                columns = ', '.join(df.columns)
            else:
                logger.warning(f"Unknown source: {source_name}")
                continue
            
            # Create INSERT statement
            placeholders = ', '.join([':' + str(i+1) for i in range(len(df.columns))])
            insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            
            # Batch insert data
            batch_size = ETL_CONFIG['commit_interval']
            total_inserted = pool.execute_batch(insert_sql, data_tuples, batch_size)
            
            logger.info(f"Staged {total_inserted} records from {source_name} to {table_name}")
        
        logger.info("Data staging completed successfully")
        
    except Exception as e:
        logger.error(f"Data staging failed: {str(e)}")
        logger.error(traceback.format_exc())
        raise
    finally:
        close_connection_pool()


def transform_and_load():
    """
    Transform staged data and load into dimensional model
    This would typically call the PL/SQL cursor-based loading procedures
    """
    logger.info("Starting transform and load process")
    
    # In a real implementation, this would:
    # 1. Call database procedures to validate staged data
    # 2. Transform data as needed
    # 3. Load dimensions (if not using cursor-based approach)
    # 4. Prepare fact table loads
    
    logger.info("Transform and load process completed")
    logger.info("NOTE: Actual loading is done via PL/SQL cursor scripts")
    logger.info("Please run 02_load_facts_cursor.sql to complete the ETL process")


def main():
    """
    Main ETL orchestration function
    """
    logger.info("=" * 60)
    logger.info("INKWAVE PUBLISHING DATA WAREHOUSE - ETL PROCESS")
    logger.info("=" * 60)
    
    try:
        # Step 1: Extract data from sources
        sources = extract_data()
        
        # Step 2: Stage data in database
        stage_data(sources)
        
        # Step 3: Transform and load (dimensional model loading)
        transform_and_load()
        
        logger.info("=" * 60)
        logger.info("ETL PROCESS COMPLETED SUCCESSFULLY")
        logger.info("=" * 60)
        logger.info("Next steps:")
        logger.info("1. Run database/etl/loading/02_load_facts_cursor.sql")
        logger.info("2. Execute analytical queries in database/queries/business_intelligence/")
        logger.info("=" * 60)
        
        return True
        
    except Exception as e:
        logger.error("=" * 60)
        logger.error("ETL PROCESS FAILED")
        logger.error("=" * 60)
        logger.error(f"Error: {str(e)}")
        logger.error(traceback.format_exc())
        logger.error("=" * 60)
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)