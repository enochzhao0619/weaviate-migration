#!/usr/bin/env python3
"""
PostgreSQL Datasets Update Script
Description: Update PostgreSQL datasets table to change vector store type from weaviate to milvus
"""

import os
import sys
import logging
from datetime import datetime
from dotenv import load_dotenv

# Add src directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from weaviate_to_zilliz_migrator import WeaviateToZillizMigrator

# Load environment variables
load_dotenv()

# Configure logging
os.makedirs('logs', exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/pg_update_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def main():
    """Main entry point for PostgreSQL datasets update"""
    logger.info("Starting PostgreSQL datasets vector store type update")
    
    # Validate required environment variables
    required_pg_vars = ['PG_HOST', 'PG_DATABASE', 'PG_USERNAME', 'PG_PASSWORD']
    required_weaviate_vars = ['WEAVIATE_ENDPOINT']
    
    missing_vars = []
    for var in required_pg_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    for var in required_weaviate_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
        logger.error("Please set these variables in your .env file or environment")
        logger.error("Required PostgreSQL variables: PG_HOST, PG_DATABASE, PG_USERNAME, PG_PASSWORD")
        logger.error("Required Weaviate variables: WEAVIATE_ENDPOINT")
        sys.exit(1)
    
    try:
        # Create migrator instance
        migrator = WeaviateToZillizMigrator()
        
        # Connect to both systems
        logger.info("Connecting to Weaviate...")
        migrator.connect_weaviate()
        
        logger.info("Connecting to PostgreSQL...")
        migrator.connect_postgresql()
        
        # Update PostgreSQL datasets
        logger.info("Starting PostgreSQL datasets update process...")
        stats = migrator.update_pg_datasets_vector_store_type()
        
        # Print final summary
        logger.info("\n" + "="*60)
        logger.info("UPDATE COMPLETED SUCCESSFULLY")
        logger.info("="*60)
        logger.info(f"Total Weaviate classes processed: {stats['total_weaviate_classes']}")
        logger.info(f"Datasets successfully updated: {stats['updated_datasets']}")
        logger.info(f"Datasets not found: {stats['not_found_datasets']}")
        logger.info(f"Failed updates: {stats['failed_updates']}")
        
        if stats['updated_datasets'] > 0:
            logger.info(f"✓ Successfully updated {stats['updated_datasets']} datasets from 'weaviate' to 'milvus' type")
        else:
            logger.warning("⚠ No datasets were updated")
        
        if stats['failed_updates'] > 0:
            logger.warning(f"⚠ {stats['failed_updates']} updates failed - check logs for details")
        
    except KeyboardInterrupt:
        logger.info("Update process interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Update process failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()