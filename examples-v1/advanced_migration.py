#!/usr/bin/env python3
"""
Advanced migration example - Demonstrates advanced features and customization
"""

import sys
import os
from pathlib import Path
import logging
from datetime import datetime

# Add src directory to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from weaviate_to_zilliz_migrator import WeaviateToZillizMigrator
from config import MigrationConfig
from data_transformer import DataTransformer
from utils import log_memory_usage

# Setup detailed logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'advanced_migration_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class AdvancedMigrator(WeaviateToZillizMigrator):
    """Extended migrator with additional features"""
    
    def __init__(self):
        super().__init__()
        self.custom_field_mappings = {}
        
    def set_custom_field_mapping(self, weaviate_field: str, zilliz_field: str):
        """Set custom field name mapping"""
        self.custom_field_mappings[weaviate_field] = zilliz_field
        logger.info(f"Custom field mapping: {weaviate_field} -> {zilliz_field}")
        
    def analyze_collection_before_migration(self, collection_name: str):
        """Analyze collection data before migration"""
        logger.info(f"Analyzing collection: {collection_name}")
        
        try:
            # Get sample data
            sample_data = self.get_collection_data(collection_name, limit=100)
            
            if not sample_data:
                logger.warning(f"No data found in {collection_name}")
                return
                
            # Get field statistics
            stats = self.transformer.get_field_statistics(sample_data)
            
            logger.info(f"Collection Analysis for {collection_name}:")
            logger.info(f"  Total documents (sample): {stats['total_documents']}")
            logger.info(f"  Vector dimensions: {stats['vector_dimensions']}")
            logger.info(f"  Field frequency:")
            
            for field, count in sorted(stats['field_frequency'].items(), key=lambda x: x[1], reverse=True)[:10]:
                percentage = (count / stats['total_documents']) * 100
                logger.info(f"    {field}: {count} ({percentage:.1f}%)")
                
            logger.info(f"  Field types:")
            for field, types in list(stats['field_types'].items())[:5]:
                logger.info(f"    {field}: {types}")
                
        except Exception as e:
            logger.error(f"Failed to analyze collection {collection_name}: {e}")
            
    def migrate_with_analysis(self, collections: list = None):
        """Migrate collections with pre-migration analysis"""
        logger.info("Starting advanced migration with analysis")
        
        # Connect to systems
        self.connect_weaviate()
        self.connect_zilliz()
        
        # Get collections
        if not collections:
            collections = self.get_weaviate_collections()
            
        # Analyze each collection first
        for collection in collections:
            self.analyze_collection_before_migration(collection)
            
        # Perform migration
        self.run_migration(collections)


def demonstrate_custom_migration():
    """Demonstrate custom migration features"""
    logger.info("=== Advanced Migration Demo ===")
    
    # Create advanced migrator
    migrator = AdvancedMigrator()
    
    # Set custom batch size for large collections
    migrator.batch_size = 50
    logger.info(f"Using custom batch size: {migrator.batch_size}")
    
    # Set custom field mappings if needed
    # migrator.set_custom_field_mapping('old_field_name', 'new_field_name')
    
    try:
        # Option 1: Analyze and migrate all collections
        migrator.migrate_with_analysis()
        
        # Option 2: Migrate specific collections with analysis
        # specific_collections = ['ImportantCollection']
        # migrator.migrate_with_analysis(specific_collections)
        
    except Exception as e:
        logger.error(f"Advanced migration failed: {e}")
        
    # Log final memory usage
    log_memory_usage()


def demonstrate_data_analysis():
    """Demonstrate data analysis capabilities"""
    logger.info("=== Data Analysis Demo ===")
    
    migrator = WeaviateToZillizMigrator()
    transformer = DataTransformer()
    
    try:
        migrator.connect_weaviate()
        collections = migrator.get_weaviate_collections()
        
        for collection in collections[:2]:  # Analyze first 2 collections
            logger.info(f"\nAnalyzing collection: {collection}")
            
            # Get schema
            schema = migrator.get_collection_schema(collection)
            schema_analysis = transformer.analyze_weaviate_schema(schema)
            
            logger.info(f"Schema properties: {len(schema_analysis['properties'])}")
            logger.info(f"Text properties: {schema_analysis['text_properties']}")
            logger.info(f"Numeric properties: {schema_analysis['numeric_properties']}")
            logger.info(f"Boolean properties: {schema_analysis['boolean_properties']}")
            
            # Get sample data
            sample_data = migrator.get_collection_data(collection, limit=10)
            if sample_data:
                stats = transformer.get_field_statistics(sample_data)
                logger.info(f"Sample size: {stats['total_documents']}")
                logger.info(f"Vector dimensions: {stats['vector_dimensions']}")
                
    except Exception as e:
        logger.error(f"Data analysis failed: {e}")


def main():
    """Main demo function"""
    print("Advanced Migration Examples")
    print("1. Custom Migration with Analysis")
    print("2. Data Analysis Demo")
    
    choice = input("Select demo (1 or 2): ").strip()
    
    if choice == "1":
        demonstrate_custom_migration()
    elif choice == "2":
        demonstrate_data_analysis()
    else:
        logger.info("Running both demos...")
        demonstrate_data_analysis()
        demonstrate_custom_migration()


if __name__ == "__main__":
    main()