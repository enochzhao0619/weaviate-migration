#!/usr/bin/env python3
"""
Simple migration example - Basic usage of the Weaviate to Zilliz migrator
"""

import sys
import os
from pathlib import Path

# Add src directory to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from weaviate_to_zilliz_migrator import WeaviateToZillizMigrator
from config import MigrationConfig
import logging

# Setup basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    """Simple migration example"""
    
    # Validate configuration
    try:
        config = MigrationConfig()
        config.validate()
        logger.info("Configuration validated successfully")
    except Exception as e:
        logger.error(f"Configuration error: {e}")
        return
    
    # Create migrator
    migrator = WeaviateToZillizMigrator()
    
    try:
        # Example 1: Migrate all collections
        logger.info("Starting migration of all collections...")
        migrator.run_migration()
        
        # Example 2: Migrate specific collections
        # specific_collections = ['MyCollection1', 'MyCollection2']
        # logger.info(f"Starting migration of specific collections: {specific_collections}")
        # migrator.run_migration(specific_collections)
        
    except Exception as e:
        logger.error(f"Migration failed: {e}")


if __name__ == "__main__":
    main()