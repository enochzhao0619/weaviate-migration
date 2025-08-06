#!/usr/bin/env python3
"""
Load All Collections Script for Zilliz Cloud
Author: Migration Tool
Description: Load all collections in Zilliz Cloud using REST API
"""

import os
import sys
import logging
from datetime import datetime
from dotenv import load_dotenv

# Add src directory to path to import migrator
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
        logging.FileHandler(f'logs/load_collections_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def main():
    """Main entry point for loading all collections"""
    logger.info("Starting load all collections process")
    
    # Validate required environment variables
    required_vars = ['ZILLIZ_CLOUD_URI', 'ZILLIZ_CLOUD_API_KEY']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
        logger.error("Please set these variables in your .env file or environment")
        sys.exit(1)
    
    try:
        # Create migrator instance (we only need Zilliz connection)
        migrator = WeaviateToZillizMigrator()
        
        # Connect to Zilliz Cloud
        migrator.connect_zilliz()
        
        # Load all collections
        migrator.load_all_collections()
        
        logger.info("Load all collections process completed successfully")
        
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Process failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main() 