#!/usr/bin/env python3
"""
Main migration script - Entry point for Weaviate to Zilliz Cloud migration
"""

import sys
import os
import argparse
import logging
from pathlib import Path

# Add src directory to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from weaviate_to_zilliz_migrator import WeaviateToZillizMigrator
from config import MigrationConfig

logger = logging.getLogger(__name__)


def setup_logging(log_level: str = 'INFO'):
    """Setup logging configuration"""
    # Create logs directory
    os.makedirs('logs', exist_ok=True)
    
    # Configure logging with proper file handler management
    file_handler = logging.FileHandler('logs/migration.log')
    stream_handler = logging.StreamHandler(sys.stdout)
    
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[file_handler, stream_handler]
    )


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Migrate vector data from Weaviate to Zilliz Cloud',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python migrate.py                           # Migrate all collections (serial batch mode)
  python migrate.py -c Collection1 Collection2  # Migrate specific collections
  python migrate.py --dry-run                 # Preview migration without executing
  python migrate.py --mode download           # Only download data from Weaviate
  python migrate.py --mode download --limit 100  # Download first 100 docs per collection
  python migrate.py --log-level DEBUG        # Enable debug logging
  python migrate.py --batch-size 500         # Use custom batch size
        """
    )
    
    parser.add_argument(
        '-c', '--collections',
        nargs='+',
        help='Specific collections to migrate (default: all collections)'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Preview migration plan without executing'
    )
    
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help='Set logging level (default: INFO)'
    )
    
    parser.add_argument(
        '--batch-size',
        type=int,
        help='Override batch size for migration'
    )
    
    parser.add_argument(
        '--skip-verification',
        action='store_true',
        help='Skip post-migration verification'
    )
    
    parser.add_argument(
        '--limit',
        type=int,
        help='Limit the number of documents to migrate per collection (for testing)'
    )
    
    parser.add_argument(
        '--mode',
        choices=['migrate', 'download'],
        default='migrate',
        help='Operation mode: migrate (full migration) or download (download data from Weaviate only)'
    )
    
    # Removed concurrent options - now only supports serial batch processing
    
    return parser.parse_args()


def validate_environment():
    """Validate environment and configuration"""
    try:
        config = MigrationConfig()
        config.validate()
        return config
    except Exception as e:
        logger.error(f"Configuration validation failed: {str(e)}")
        logger.error("Please check your environment variables or .env file")
        return None


def download_data_only(migrator: WeaviateToZillizMigrator, collections: list = None, limit: int = None):
    """Download data from Weaviate only without migration"""
    logger.info("="*60)
    logger.info("DOWNLOAD MODE - WEAVIATE DATA ONLY")
    logger.info("="*60)
    
    try:
        # Connect to Weaviate only
        migrator.connect_weaviate()
        
        # Get collections
        if not collections:
            collections = migrator.get_weaviate_collections()
            
        logger.info(f"Collections to download: {len(collections)}")
        
        # Create data directory
        os.makedirs('downloaded_data', exist_ok=True)
        
        for collection in collections:
            logger.info(f"\nDownloading collection: {collection}")
            
            try:
                # Get collection data using cursor pagination
                data = migrator.get_collection_data(collection, limit=limit, show_progress=True)
                
                if data:
                    # Save to JSON file
                    filename = f"downloaded_data/{collection}_data.json"
                    with open(filename, 'w', encoding='utf-8') as f:
                        import json
                        json.dump(data, f, indent=2, ensure_ascii=False, default=str)
                    
                    logger.info(f"  ✓ Downloaded {len(data)} documents to {filename}")
                    
                    # Get schema info
                    schema = migrator.get_collection_schema(collection)
                    if schema:
                        schema_filename = f"downloaded_data/{collection}_schema.json"
                        with open(schema_filename, 'w', encoding='utf-8') as f:
                            json.dump(schema, f, indent=2, ensure_ascii=False, default=str)
                        logger.info(f"  ✓ Downloaded schema to {schema_filename}")
                else:
                    logger.warning(f"  ⚠ No data found in collection {collection}")
                    
            except Exception as e:
                logger.error(f"  ✗ Failed to download {collection}: {str(e)}")
                
        logger.info("\n" + "="*60)
        logger.info("Download completed. Data saved to 'downloaded_data' directory.")
        
    except Exception as e:
        logger.error(f"Download failed: {str(e)}")


def preview_migration(migrator: WeaviateToZillizMigrator, collections: list = None):
    """Preview migration plan without executing"""
    logger.info("="*60)
    logger.info("MIGRATION PREVIEW")
    logger.info("="*60)
    
    try:
        # Connect to Weaviate only
        migrator.connect_weaviate()
        
        # Get collections
        if not collections:
            collections = migrator.get_weaviate_collections()
            
        logger.info(f"Collections to migrate: {len(collections)}")
        
        for collection in collections:
            logger.info(f"\nCollection: {collection}")
            
            # Get schema info
            schema = migrator.get_collection_schema(collection)
            if schema and 'properties' in schema:
                properties = schema['properties']
                
                # Handle both dict and list formats for properties
                if isinstance(properties, dict):
                    logger.info(f"  Properties: {len(properties)}")
                    for prop_name, prop_info in list(properties.items())[:5]:
                        data_types = prop_info.get('dataType', ['unknown'])
                        logger.info(f"    - {prop_name}: {data_types[0]}")
                    if len(properties) > 5:
                        logger.info(f"    ... and {len(properties) - 5} more")
                elif isinstance(properties, list):
                    logger.info(f"  Properties: {len(properties)}")
                    for prop_info in properties[:5]:
                        prop_name = prop_info.get('name', 'unknown')
                        data_types = prop_info.get('dataType', ['unknown'])
                        logger.info(f"    - {prop_name}: {data_types[0] if data_types else 'unknown'}")
                    if len(properties) > 5:
                        logger.info(f"    ... and {len(properties) - 5} more")
                else:
                    logger.info(f"  Properties: Unknown format ({type(properties)})")
                    
            # Get document count
            data = migrator.get_collection_data(collection, limit=1)
            if data:
                # Get actual count by querying without limit
                all_data = migrator.get_collection_data(collection)
                logger.info(f"  Documents: {len(all_data)}")
                
                # Get vector dimension
                vector = data[0]['_additional'].get('vector')
                if vector:
                    logger.info(f"  Vector dimension: {len(vector)}")
            else:
                logger.info(f"  Documents: 0")
                
        logger.info("\n" + "="*60)
        logger.info("Preview completed. Use --dry-run=false to execute migration.")
        
    except Exception as e:
        logger.error(f"Preview failed: {str(e)}")


def main():
    """Main entry point"""
    args = parse_arguments()
    
    # Setup logging
    setup_logging(args.log_level)
    
    logger.info("Starting Weaviate to Zilliz Cloud Migration Tool")
    logger.info(f"Arguments: {vars(args)}")
    
    # Validate configuration
    config = validate_environment()
    if not config:
        sys.exit(1)
        
    # Create migrator
    migrator = WeaviateToZillizMigrator()
    
    # Override batch size if specified
    if args.batch_size:
        migrator.batch_size = args.batch_size
        logger.info(f"Using custom batch size: {args.batch_size}")
        
    try:
        logger.info("Using serial batch processing mode")
        logger.info(f"Batch size: {migrator.batch_size} documents per batch")
            
        if args.dry_run:
            # Preview mode
            preview_migration(migrator, args.collections)
        elif args.mode == 'download':
            # Download mode - only download data from Weaviate
            download_data_only(migrator, args.collections, args.limit)
        else:
            # Full migration mode - serial batch processing
            migrator.run_migration(args.collections, limit=args.limit)
            
            if not args.skip_verification:
                logger.info("Migration completed successfully!")
            else:
                logger.info("Migration completed (verification skipped)")
                
    except KeyboardInterrupt:
        logger.info("Migration interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Migration failed: {str(e)}")
        logger.debug("Full error details:", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()