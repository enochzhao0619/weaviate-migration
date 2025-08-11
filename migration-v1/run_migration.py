#!/usr/bin/env python3
"""
Simplified migration runner for Weaviate to Zilliz Cloud migration
Serial batch processing with 250 documents per batch
"""

import sys
import os
import logging
from pathlib import Path

# Add src directory to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from weaviate_to_zilliz_migrator import WeaviateToZillizMigrator

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/migration.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def main():
    """Simple migration runner"""
    # Create logs directory
    os.makedirs('logs', exist_ok=True)
    
    logger.info("="*60)
    logger.info("Weaviate to Zilliz Cloud Migration Tool")
    logger.info("Serial Batch Processing Mode")
    logger.info("="*60)
    
    # Parse simple command line arguments
    collections_to_migrate = None
    limit = None
    
    if len(sys.argv) > 1:
        if sys.argv[1] == '--help' or sys.argv[1] == '-h':
            print("""
Usage: python run_migration.py [options]

Options:
  --help, -h          Show this help message
  --limit N           Limit migration to N documents per collection
  --collections C1 C2 Migrate only specified collections
  
Examples:
  python run_migration.py                    # Migrate all collections
  python run_migration.py --limit 1000      # Migrate max 1000 docs per collection
  python run_migration.py --collections Collection1 Collection2
            """)
            return
            
        if '--limit' in sys.argv:
            limit_index = sys.argv.index('--limit')
            if limit_index + 1 < len(sys.argv):
                limit = int(sys.argv[limit_index + 1])
                logger.info(f"Limiting migration to {limit} documents per collection")
                
        if '--collections' in sys.argv:
            collections_index = sys.argv.index('--collections')
            collections_to_migrate = sys.argv[collections_index + 1:]
            logger.info(f"Will migrate specific collections: {collections_to_migrate}")
    
    try:
        # Create migrator
        migrator = WeaviateToZillizMigrator()
        logger.info(f"Using batch size: {migrator.batch_size} documents per batch")
        
        # Run migration
        migrator.run_migration(collections=collections_to_migrate, limit=limit)
        
        logger.info("="*60)
        logger.info("Migration completed successfully!")
        logger.info("="*60)
        
    except KeyboardInterrupt:
        logger.info("Migration interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Migration failed: {str(e)}")
        logger.error("Check the log file for detailed error information")
        sys.exit(1)

if __name__ == "__main__":
    main()
