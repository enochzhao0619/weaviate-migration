#!/usr/bin/env python3
"""
Test script for load collections functionality
"""

import os
import sys
import logging
from dotenv import load_dotenv

# Add src directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))
from weaviate_to_zilliz_migrator import WeaviateToZillizMigrator

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_get_collections():
    """Test getting collections from Zilliz Cloud"""
    logger.info("Testing get_zilliz_collections method...")
    
    migrator = WeaviateToZillizMigrator()
    
    try:
        migrator.connect_zilliz()
        collections = migrator.get_zilliz_collections()
        
        logger.info(f"Found collections: {collections}")
        logger.info(f"Total collections: {len(collections)}")
        
        return collections
        
    except Exception as e:
        logger.error(f"Test failed: {str(e)}")
        return []


def test_load_single_collection(collection_name: str):
    """Test loading a single collection"""
    logger.info(f"Testing load_collection method for: {collection_name}")
    
    migrator = WeaviateToZillizMigrator()
    
    try:
        migrator.connect_zilliz()
        migrator.load_collection(collection_name)
        logger.info(f"Successfully tested loading collection: {collection_name}")
        
    except Exception as e:
        logger.error(f"Test failed for collection {collection_name}: {str(e)}")


def test_load_all_collections():
    """Test loading all collections"""
    logger.info("Testing load_all_collections method...")
    
    migrator = WeaviateToZillizMigrator()
    
    try:
        migrator.connect_zilliz()
        migrator.load_all_collections()
        logger.info("Successfully tested load_all_collections")
        
    except Exception as e:
        logger.error(f"Test failed: {str(e)}")


def main():
    """Run all tests"""
    logger.info("Starting load collections functionality tests")
    
    # Test 1: Get collections
    collections = test_get_collections()
    
    if collections:
        # Test 2: Load first collection individually
        test_load_single_collection(collections[0])
        
        # Test 3: Load all collections
        test_load_all_collections()
    else:
        logger.warning("No collections found, skipping load tests")
    
    logger.info("Tests completed")


if __name__ == "__main__":
    main() 