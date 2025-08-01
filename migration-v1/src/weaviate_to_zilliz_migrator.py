#!/usr/bin/env python3
"""
Weaviate to Zilliz Cloud Migration Script
Author: Dify Migration Tool
Description: Migrate vector data from Weaviate to Zilliz Cloud (Milvus)
"""

import os
import json
import logging
import sys
from typing import List, Dict, Any, Optional
from datetime import datetime
import weaviate
from pymilvus import MilvusClient, DataType, CollectionSchema, FieldSchema
from dotenv import load_dotenv
import time
from tqdm import tqdm
import numpy as np
from data_transformer import DataTransformer
from utils import retry_on_failure, log_memory_usage, create_safe_collection_name

# Load environment variables
load_dotenv()

# Configure logging
import os
os.makedirs('logs', exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/migration_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class WeaviateToZillizMigrator:
    """Migration tool for transferring data from Weaviate to Zilliz Cloud"""
    
    def __init__(self):
        # Weaviate configuration
        self.weaviate_url = os.getenv('WEAVIATE_ENDPOINT', 'http://localhost:8080')
        self.weaviate_api_key = os.getenv('WEAVIATE_API_KEY', '')
        
        # Zilliz Cloud configuration
        self.zilliz_uri = os.getenv('ZILLIZ_CLOUD_URI', '')
        self.zilliz_token = os.getenv('ZILLIZ_CLOUD_API_KEY', '')
        self.zilliz_db_name = os.getenv('ZILLIZ_CLOUD_DATABASE', 'default')
        
        # Migration configuration
        self.batch_size = int(os.getenv('MIGRATION_BATCH_SIZE', '100'))
        self.dimension = None
        
        # Client instances
        self.weaviate_client = None
        self.zilliz_client = None
        
        # Data transformer
        self.transformer = DataTransformer()
        
        # Migration statistics
        self.migration_stats = {
            'start_time': None,
            'end_time': None,
            'total_collections': 0,
            'successful_collections': [],
            'failed_collections': [],
            'total_documents': 0,
            'migrated_documents': 0
        }
        
    def connect_weaviate(self):
        """Establish connection to Weaviate using v3 client"""
        try:
            # Configure authentication for v3 client
            if self.weaviate_api_key:
                auth_config = weaviate.AuthApiKey(api_key=self.weaviate_api_key)
                self.weaviate_client = weaviate.Client(
                    url=self.weaviate_url,
                    auth_client_secret=auth_config,
                    timeout_config=(60, 60)  # (connect_timeout, read_timeout)
                )
            else:
                self.weaviate_client = weaviate.Client(
                    url=self.weaviate_url,
                    timeout_config=(60, 60)
                )
            
            # Test connection
            if self.weaviate_client.is_ready():
                logger.info(f"Successfully connected to Weaviate at {self.weaviate_url}")
                # Get Weaviate version info
                meta = self.weaviate_client.get_meta()
                logger.info(f"Weaviate version: {meta.get('version', 'unknown')}")
            else:
                raise Exception("Weaviate is not ready")
                
        except Exception as e:
            logger.error(f"Failed to connect to Weaviate: {str(e)}")
            raise
            
    def connect_zilliz(self):
        """Establish connection to Zilliz Cloud"""
        try:
            self.zilliz_client = MilvusClient(
                uri=self.zilliz_uri,
                token=self.zilliz_token,
                db_name=self.zilliz_db_name
            )
            
            # Test connection by listing collections
            self.zilliz_client.list_collections()
            logger.info(f"Successfully connected to Zilliz Cloud")
            
        except Exception as e:
            logger.error(f"Failed to connect to Zilliz Cloud: {str(e)}")
            raise
            
    def get_weaviate_collections(self) -> List[str]:
        """Get all collection names from Weaviate using v3 client"""
        try:
            schema = self.weaviate_client.schema.get()
            collections = [cls['class'] for cls in schema.get('classes', [])]
            logger.info(f"Found {len(collections)} collections in Weaviate: {collections}")
            return collections
        except Exception as e:
            logger.error(f"Failed to get Weaviate collections: {str(e)}")
            raise
            
    def get_collection_schema(self, collection_name: str) -> Dict[str, Any]:
        """Get schema information for a specific collection using v3 client"""
        try:
            schema = self.weaviate_client.schema.get(collection_name)
            logger.info(f"Retrieved schema for collection {collection_name}")
            return schema
        except Exception as e:
            logger.error(f"Failed to get schema for collection {collection_name}: {str(e)}")
            return {}
            
    def get_collection_data(self, collection_name: str, limit: int = None) -> List[Dict[str, Any]]:
        """Retrieve all data from a Weaviate collection using v3 client"""
        try:
            # Build GraphQL query for v3 client
            additional_fields = ["id", "vector"]
            
            # Get schema to understand properties
            schema = self.get_collection_schema(collection_name)
            properties = []
            if schema and 'properties' in schema:
                schema_properties = schema['properties']
                if isinstance(schema_properties, dict):
                    properties = list(schema_properties.keys())
                elif isinstance(schema_properties, list):
                    properties = [prop.get('name') for prop in schema_properties if prop.get('name')]
            
            # Build query
            query_builder = self.weaviate_client.query.get(collection_name, properties).with_additional(additional_fields)
            
            if limit:
                query_builder = query_builder.with_limit(limit)
            
            result = query_builder.do()
            
            # Extract data from GraphQL response
            data = []
            if result and 'data' in result and 'Get' in result['data'] and collection_name in result['data']['Get']:
                objects = result['data']['Get'][collection_name]
                for obj in objects:
                    # Separate properties from additional fields
                    obj_data = {k: v for k, v in obj.items() if k != '_additional'}
                    obj_data['_additional'] = obj.get('_additional', {})
                    data.append(obj_data)
            
            logger.info(f"Retrieved {len(data)} documents from {collection_name}")
            return data
        except Exception as e:
            logger.error(f"Failed to get data from collection {collection_name}: {str(e)}")
            raise
            
    def create_zilliz_collection(self, collection_name: str, dimension: int, schema_info: Dict[str, Any] = None):
        """Create a collection in Zilliz Cloud with the same schema"""
        try:
            # Ensure collection name is safe
            safe_collection_name = create_safe_collection_name(collection_name)
            if safe_collection_name != collection_name:
                logger.info(f"Collection name sanitized: {collection_name} -> {safe_collection_name}")
                collection_name = safe_collection_name
            
            # Check if collection already exists
            if self.zilliz_client.has_collection(collection_name):
                logger.warning(f"Collection {collection_name} already exists in Zilliz Cloud")
                # For automated migration, drop and recreate
                self.zilliz_client.drop_collection(collection_name)
                logger.info(f"Dropped existing collection {collection_name}")
                
            # Use transformer to create schema fields
            fields = self.transformer.create_zilliz_schema_fields(schema_info or {}, dimension)
            
            schema = CollectionSchema(fields=fields, description=f"Migrated from Weaviate: {collection_name}")
            
            # Create collection
            self.zilliz_client.create_collection(
                collection_name=collection_name,
                schema=schema,
                consistency_level="Strong"
            )
            
            # Create index on vector field using prepare_index_params
            index_params = self.zilliz_client.prepare_index_params()
            index_params.add_index(
                field_name="vector",
                index_type="HNSW",
                metric_type="IP",
                params={"M": 16, "efConstruction": 64}
            )
            
            self.zilliz_client.create_index(
                collection_name=collection_name,
                index_params=index_params
            )
            
            logger.info(f"Successfully created collection {collection_name} in Zilliz Cloud")
            
        except Exception as e:
            logger.error(f"Failed to create collection {collection_name}: {str(e)}")
            raise
            
    @retry_on_failure(max_retries=3, delay=1.0)
    def transform_data_for_zilliz(self, weaviate_data: List[Dict[str, Any]], collection_schema: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Transform Weaviate data format to Zilliz format using the transformer"""
        try:
            # Analyze schema for transformation
            schema_analysis = self.transformer.analyze_weaviate_schema(collection_schema)
            
            # Transform batch
            zilliz_data = self.transformer.transform_batch(weaviate_data, schema_analysis)
            
            # Validate transformed data
            valid_data, errors = self.transformer.validate_transformed_data(zilliz_data)
            
            if errors:
                logger.warning(f"Data validation issues: {len(errors)} documents had problems")
                for error in errors[:5]:  # Show first 5 errors
                    logger.warning(f"  {error}")
                if len(errors) > 5:
                    logger.warning(f"  ... and {len(errors) - 5} more errors")
                    
            return valid_data
            
        except Exception as e:
            logger.error(f"Failed to transform data batch: {str(e)}")
            raise
            
    def migrate_collection(self, collection_name: str, limit: int = None):
        """Migrate a single collection from Weaviate to Zilliz Cloud"""
        logger.info(f"Starting migration for collection: {collection_name}")
        
        try:
            # Get collection schema
            schema_info = self.get_collection_schema(collection_name)
            
            # Get all data from Weaviate (with optional limit)
            weaviate_data = self.get_collection_data(collection_name, limit=limit)
            
            if not weaviate_data:
                logger.warning(f"No data found in collection {collection_name}")
                return 0
                
            # Extract dimension from first vector
            first_vector = weaviate_data[0]['_additional'].get('vector')
            if not first_vector:
                logger.error(f"No vector found in collection {collection_name}")
                return 0
                
            dimension = len(first_vector)
            logger.info(f"Vector dimension: {dimension}")
            
            # Create collection in Zilliz
            self.create_zilliz_collection(collection_name, dimension, schema_info)
            
            # Log memory usage before migration
            log_memory_usage()
            
            # Transform and migrate data in batches
            total_docs = len(weaviate_data)
            migrated_count = 0
            
            with tqdm(total=total_docs, desc=f"Migrating {collection_name}") as pbar:
                for i in range(0, total_docs, self.batch_size):
                    batch = weaviate_data[i:i + self.batch_size]
                    
                    # Transform data for Zilliz
                    zilliz_batch = self.transform_data_for_zilliz(batch, schema_info)
                    
                    if not zilliz_batch:
                        logger.warning(f"No valid data in batch {i//self.batch_size + 1}")
                        pbar.update(len(batch))
                        continue
                    
                    # Insert into Zilliz
                    try:
                        self.zilliz_client.insert(
                            collection_name=collection_name,
                            data=zilliz_batch
                        )
                        migrated_count += len(zilliz_batch)
                        pbar.update(len(batch))
                        
                        # Small delay to avoid overwhelming the server
                        time.sleep(0.1)
                        
                    except Exception as e:
                        logger.error(f"Failed to insert batch {i//self.batch_size + 1}: {str(e)}")
                        pbar.update(len(batch))
                        continue
                        
            logger.info(f"Successfully migrated {migrated_count}/{total_docs} documents in {collection_name}")
            return migrated_count
            
        except Exception as e:
            logger.error(f"Failed to migrate collection {collection_name}: {str(e)}")
            raise
            
    def verify_migration(self, collection_name: str) -> bool:
        """Verify the migration by comparing document counts"""
        try:
            # Get Weaviate count
            weaviate_data = self.get_collection_data(collection_name)
            weaviate_count = len(weaviate_data)
            
            # Get Zilliz count
            result = self.zilliz_client.query(
                collection_name=collection_name,
                filter="",
                output_fields=["count(*)"],
                limit=1
            )
            
            zilliz_count = 0
            if result and len(result) > 0:
                zilliz_count = result[0].get('count(*)', 0)
            
            logger.info(f"Verification for {collection_name}:")
            logger.info(f"  Weaviate documents: {weaviate_count}")
            logger.info(f"  Zilliz documents: {zilliz_count}")
            
            if weaviate_count == zilliz_count:
                logger.info(f"✓ Migration verified successfully for {collection_name}")
                return True
            else:
                logger.warning(f"✗ Document count mismatch for {collection_name}")
                return False
                
        except Exception as e:
            logger.error(f"Failed to verify migration for {collection_name}: {str(e)}")
            return False
            
    def export_migration_report(self):
        """Export detailed migration report"""
        try:
            report = {
                'migration_summary': self.migration_stats,
                'timestamp': datetime.now().isoformat(),
                'configuration': {
                    'weaviate_endpoint': self.weaviate_url,
                    'zilliz_uri': self.zilliz_uri,
                    'batch_size': self.batch_size
                }
            }
            
            report_file = f"reports/migration_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            os.makedirs('reports', exist_ok=True)
            
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, ensure_ascii=False)
                
            logger.info(f"Migration report exported to {report_file}")
            
        except Exception as e:
            logger.error(f"Failed to export migration report: {str(e)}")
            
    def run_migration(self, collections: Optional[List[str]] = None, limit: int = None):
        """Run the complete migration process"""
        logger.info("Starting Weaviate to Zilliz Cloud migration")
        if limit:
            logger.info(f"Limiting migration to {limit} documents per collection")
        self.migration_stats['start_time'] = datetime.now()
        
        try:
            # Connect to both systems
            self.connect_weaviate()
            self.connect_zilliz()
            
            # Get collections to migrate
            if not collections:
                collections = self.get_weaviate_collections()
                
            self.migration_stats['total_collections'] = len(collections)
            logger.info(f"Will migrate {len(collections)} collections: {collections}")
            
            # Migrate each collection
            for collection in collections:
                try:
                    logger.info(f"\n{'='*60}")
                    logger.info(f"Processing collection: {collection}")
                    logger.info(f"{'='*60}")
                    
                    migrated_docs = self.migrate_collection(collection, limit=limit)
                    self.migration_stats['migrated_documents'] += migrated_docs
                    
                    if self.verify_migration(collection):
                        self.migration_stats['successful_collections'].append(collection)
                    else:
                        self.migration_stats['failed_collections'].append(collection)
                        
                except Exception as e:
                    logger.error(f"Migration failed for {collection}: {str(e)}")
                    self.migration_stats['failed_collections'].append(collection)
                    
            self.migration_stats['end_time'] = datetime.now()
            
            # Print summary
            self._print_migration_summary()
            
            # Export report
            self.export_migration_report()
            
        except Exception as e:
            logger.error(f"Migration process failed: {str(e)}")
            raise
            
    def _print_migration_summary(self):
        """Print detailed migration summary"""
        stats = self.migration_stats
        duration = stats['end_time'] - stats['start_time']
        
        logger.info("\n" + "="*60)
        logger.info("MIGRATION SUMMARY")
        logger.info("="*60)
        logger.info(f"Start time: {stats['start_time'].strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"End time: {stats['end_time'].strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Duration: {duration}")
        logger.info(f"Total collections: {stats['total_collections']}")
        logger.info(f"Successful: {len(stats['successful_collections'])}")
        logger.info(f"Failed: {len(stats['failed_collections'])}")
        logger.info(f"Total documents migrated: {stats['migrated_documents']}")
        
        if stats['successful_collections']:
            logger.info(f"\nSuccessful collections:")
            for collection in stats['successful_collections']:
                logger.info(f"  ✓ {collection}")
                
        if stats['failed_collections']:
            logger.warning(f"\nFailed collections:")
            for collection in stats['failed_collections']:
                logger.warning(f"  ✗ {collection}")
                
        logger.info("\nMigration process completed")


def main():
    """Main entry point"""
    # Create necessary directories
    os.makedirs('logs', exist_ok=True)
    os.makedirs('reports', exist_ok=True)
    
    # Validate required environment variables
    required_vars = ['ZILLIZ_CLOUD_URI', 'ZILLIZ_CLOUD_API_KEY']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
        logger.error("Please set these variables in your .env file or environment")
        sys.exit(1)
        
    # Create migrator instance
    migrator = WeaviateToZillizMigrator()
    
    # Run migration
    try:
        # You can specify specific collections to migrate
        # collections_to_migrate = ['Vector_index_abc123_Node']
        # migrator.run_migration(collections_to_migrate)
        
        # Or migrate all collections
        migrator.run_migration()
        
    except KeyboardInterrupt:
        logger.info("Migration interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Migration failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()