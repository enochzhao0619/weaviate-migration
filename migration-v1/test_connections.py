#!/usr/bin/env python3
"""
Connection test script - Verify connections to Weaviate and Zilliz Cloud using v3 client
"""

import sys
import os
from pathlib import Path
import logging

# Add src directory to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from config import MigrationConfig
import weaviate
from pymilvus import MilvusClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_weaviate_connection():
    """Test connection to Weaviate using v3 client"""
    logger.info("Testing Weaviate connection (v3 client)...")
    
    try:
        config = MigrationConfig()
        
        # Configure authentication for v3 client
        if config.weaviate_api_key:
            auth_config = weaviate.AuthApiKey(api_key=config.weaviate_api_key)
            client = weaviate.Client(
                url=config.weaviate_endpoint,
                auth_client_secret=auth_config,
                timeout_config=(60, 60)  # (connect_timeout, read_timeout)
            )
        else:
            client = weaviate.Client(
                url=config.weaviate_endpoint,
                timeout_config=(60, 60)
            )
        
        # Test connection
        if client.is_ready():
            logger.info("✓ Weaviate connection successful")
            
            # Get Weaviate version info
            meta = client.get_meta()
            logger.info(f"✓ Weaviate version: {meta.get('version', 'unknown')}")
            
            # Get collections using v3 client
            schema = client.schema.get()
            collections = [cls['class'] for cls in schema.get('classes', [])]
            logger.info(f"✓ Found {len(collections)} collections: {collections}")
            
            return True
        else:
            logger.error("✗ Weaviate is not ready")
            return False
            
    except Exception as e:
        logger.error(f"✗ Weaviate connection failed: {str(e)}")
        return False


def test_zilliz_connection():
    """Test connection to Zilliz Cloud"""
    logger.info("Testing Zilliz Cloud connection...")
    
    try:
        config = MigrationConfig()
        
        # Create Zilliz client
        client = MilvusClient(
            uri=config.zilliz_uri,
            token=config.zilliz_token,
            db_name=config.zilliz_db_name
        )
        
        # Test connection by listing collections
        collections = client.list_collections()
        logger.info("✓ Zilliz Cloud connection successful")
        logger.info(f"✓ Found {len(collections)} collections: {collections}")
        
        return True
        
    except Exception as e:
        logger.error(f"✗ Zilliz Cloud connection failed: {str(e)}")
        return False


def test_configuration():
    """Test configuration validation"""
    logger.info("Testing configuration...")
    
    try:
        config = MigrationConfig()
        config.validate()
        logger.info("✓ Configuration validation successful")
        
        # Print configuration summary
        config_dict = config.to_dict()
        logger.info("Configuration summary:")
        logger.info(f"  Weaviate endpoint: {config_dict['weaviate']['endpoint']}")
        logger.info(f"  Weaviate has API key: {config_dict['weaviate']['has_api_key']}")
        logger.info(f"  Zilliz URI: {config_dict['zilliz']['uri']}")
        logger.info(f"  Zilliz database: {config_dict['zilliz']['database']}")
        logger.info(f"  Zilliz has token: {config_dict['zilliz']['has_token']}")
        logger.info(f"  Batch size: {config_dict['migration']['batch_size']}")
        
        return True
        
    except Exception as e:
        logger.error(f"✗ Configuration validation failed: {str(e)}")
        return False


def main():
    """Main test function"""
    logger.info("="*60)
    logger.info("CONNECTION TEST SUITE (Weaviate v3 Client)")
    logger.info("="*60)
    
    results = {
        'configuration': test_configuration(),
        'weaviate': test_weaviate_connection(),
        'zilliz': test_zilliz_connection()
    }
    
    logger.info("\n" + "="*60)
    logger.info("TEST RESULTS SUMMARY")
    logger.info("="*60)
    
    for test_name, result in results.items():
        status = "✓ PASS" if result else "✗ FAIL"
        logger.info(f"{test_name.capitalize()}: {status}")
        
    all_passed = all(results.values())
    
    if all_passed:
        logger.info("\n🎉 All tests passed! You're ready to run the migration.")
    else:
        logger.error("\n❌ Some tests failed. Please check your configuration and connections.")
        logger.error("Common issues:")
        logger.error("  - Check your .env file exists and has correct values")
        logger.error("  - Verify API keys and endpoints are correct")
        logger.error("  - Ensure services are running and accessible")
        logger.error("  - For Weaviate 1.19.1, make sure you're using the correct IP and token")
        
    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())