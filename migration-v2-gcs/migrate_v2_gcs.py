#!/usr/bin/env python3
"""
GCS-based Migration Script (v2) - Migrate via Google Cloud Storage
Description: Backup Weaviate data to GCS, then import to Zilliz Cloud via GCS
"""

import os
import json
import random
import threading
import time
import pandas as pd
import numpy as np
import logging
import sys
import argparse
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime

from pymilvus import (
    FieldSchema, CollectionSchema, DataType,
    connections,
    Collection,
    utility,
    BulkInsertState,
)
import weaviate
from pymilvus.bulk_writer import RemoteBulkWriter
# Use `from pymilvus import RemoteBulkWriter` 
# when you use pymilvus earlier than 2.4.2 

from pymilvus.bulk_writer import BulkFileType
# Use `from pymilvus import BulkFileType` 
# when you use pymilvus earlier than 2.4.2 

from pymilvus.bulk_writer import (
    LocalBulkWriter,
    list_import_jobs,
    bulk_import,
    get_import_progress,
)

# Utility functions
def retry_on_failure(max_retries: int = 3, delay: float = 1.0, backoff: float = 2.0):
    """Decorator for retrying failed operations"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < max_retries:
                        wait_time = delay * (backoff ** attempt)
                        logger.warning(f"Attempt {attempt + 1} failed: {str(e)}. Retrying in {wait_time:.1f}s...")
                        time.sleep(wait_time)
                    else:
                        logger.error(f"All {max_retries + 1} attempts failed")
            raise last_exception
        return wrapper
    return decorator

def log_memory_usage():
    """Log current memory usage"""
    pass

def create_safe_collection_name(original_name: str) -> str:
    """Create a safe collection name for Milvus"""
    import re
    safe_name = re.sub(r'[^a-zA-Z0-9_]', '_', original_name)
    if safe_name and safe_name[0].isdigit():
        safe_name = f"collection_{safe_name}"
    return safe_name or "default_collection"


class MigrationConfig:
    """Configuration class for migration settings"""
    
    def __init__(self):
        # Weaviate configuration
        self.weaviate_endpoint = os.getenv('WEAVIATE_ENDPOINT', 'http://localhost:8080')
        self.weaviate_api_key = os.getenv('WEAVIATE_API_KEY', '')
        
        # Zilliz Cloud configuration
        self.zilliz_uri = os.getenv('ZILLIZ_CLOUD_URI', '')
        self.zilliz_token = os.getenv('ZILLIZ_CLOUD_API_KEY', '')
        self.zilliz_db_name = os.getenv('ZILLIZ_CLOUD_DATABASE', 'default')
        
        # Migration configuration
        self.batch_size = int(os.getenv('MIGRATION_BATCH_SIZE', '100'))
        self.max_retries = int(os.getenv('MIGRATION_MAX_RETRIES', '3'))
        self.retry_delay = float(os.getenv('MIGRATION_RETRY_DELAY', '1.0'))
        
        # Index configuration
        self.index_type = os.getenv('ZILLIZ_INDEX_TYPE', 'HNSW')
        self.metric_type = os.getenv('ZILLIZ_METRIC_TYPE', 'IP')
        self.index_params = {
            'M': int(os.getenv('ZILLIZ_INDEX_M', '16')),
            'efConstruction': int(os.getenv('ZILLIZ_INDEX_EF_CONSTRUCTION', '64'))
        }
        
    def validate(self) -> bool:
        """Validate configuration settings"""
        required_fields = [
            ('zilliz_uri', self.zilliz_uri),
            ('zilliz_token', self.zilliz_token)
        ]
        
        missing_fields = [field for field, value in required_fields if not value]
        
        if missing_fields:
            raise ValueError(f"Missing required configuration: {', '.join(missing_fields)}")
            
        return True

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/gcs_migration_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class GCSMigrationConfig(MigrationConfig):
    """Extended configuration for GCS-based migration"""
    
    def __init__(self):
        super().__init__()
        
        # GCS configuration
        self.gcs_bucket_name = os.getenv('GCS_BUCKET_NAME', '')
        self.gcs_key_path = os.getenv('GCS_KEY_PATH', '')
        self.gcs_remote_path = os.getenv('GCS_REMOTE_PATH', 'bulk_data_gcs')
        
        # Bulk writer configuration
        self.segment_size = int(os.getenv('GCS_SEGMENT_SIZE', str(512*1024*1024)))  # 512MB
        self.file_type = BulkFileType.PARQUET  # Default to PARQUET
        
    def validate(self) -> bool:
        """Validate GCS-specific configuration"""
        super().validate()
        
        required_gcs_fields = [
            ('gcs_bucket_name', self.gcs_bucket_name),
            ('gcs_key_path', self.gcs_key_path)
        ]
        
        missing_fields = [field for field, value in required_gcs_fields if not value]
        
        if missing_fields:
            raise ValueError(f"Missing required GCS configuration: {', '.join(missing_fields)}")
            
        # Check if GCS key file exists
        if not os.path.exists(self.gcs_key_path):
            raise ValueError(f"GCS key file not found: {self.gcs_key_path}")
            
        return True


class WeaviateToZillizGCSMigrator:
    """GCS-based migration tool for transferring data from Weaviate to Zilliz Cloud via GCS"""
    
    def __init__(self, config: GCSMigrationConfig = None):
        self.config = config or GCSMigrationConfig()
        
        # Client instances
        self.weaviate_client = None
        self.zilliz_connected = False
        
        # Migration statistics
        self.migration_stats = {
            'start_time': None,
            'end_time': None,
            'total_collections': 0,
            'successful_collections': [],
            'failed_collections': [],
            'total_documents': 0,
            'migrated_documents': 0,
            'gcs_files': []
        }
        
    def create_zilliz_connection(self):
        """Create connection to Zilliz Cloud"""
        logger.info("Creating connection to Zilliz Cloud...")
        try:
            connections.connect(
                uri=self.config.zilliz_uri, 
                token=self.config.zilliz_token
            )
            self.zilliz_connected = True
            logger.info("Successfully connected to Zilliz Cloud")
        except Exception as e:
            logger.error(f"Failed to connect to Zilliz Cloud: {str(e)}")
            raise
    
    def connect_weaviate(self):
        """Establish connection to Weaviate"""
        try:
            # Parse Weaviate endpoint to extract host and port
            endpoint = self.config.weaviate_endpoint.replace('http://', '').replace('https://', '')
            if ':' in endpoint:
                host, port = endpoint.split(':')
                port = int(port)
            else:
                host = endpoint
                port = 8080
            
            # Determine gRPC port (typically HTTP port + 42031)
            grpc_port = port + 42031 if port == 8080 else 50051
            
            if self.config.weaviate_api_key:
                self.weaviate_client = weaviate.connect_to_custom(
                    http_host=host,
                    http_port=port,
                    http_secure=False,
                    grpc_host=host,
                    grpc_port=grpc_port,
                    grpc_secure=False,
                    auth_credentials=weaviate.AuthApiKey(self.config.weaviate_api_key)
                )
            else:
                self.weaviate_client = weaviate.connect_to_local(
                    host=host, 
                    port=port, 
                    grpc_port=grpc_port
                )
            
            logger.info(f"Successfully connected to Weaviate at {host}:{port}")
            
        except Exception as e:
            logger.error(f"Failed to connect to Weaviate: {str(e)}")
            raise
    
    def get_or_create_collection_schema(self, collection_name: str) -> CollectionSchema:
        """Get existing collection schema or create a simple one"""
        logger.info(f"Getting collection schema for: {collection_name}")
        
        try:
            # Try to get existing collection
            if utility.has_collection(collection_name):
                collection = Collection(collection_name)
                return collection.schema
            else:
                logger.warning(f"Collection {collection_name} does not exist, creating default schema")
                # Create a default schema - you may need to customize this
                fields = [
                    FieldSchema(name="id", dtype=DataType.VARCHAR, is_primary=True, max_length=512),
                    FieldSchema(name="vector", dtype=DataType.FLOAT_VECTOR, dim=768)  # Default dimension
                ]
                schema = CollectionSchema(fields, f"Schema for {collection_name}")
                return schema
                
        except Exception as e:
            logger.error(f"Failed to get schema for {collection_name}: {str(e)}")
            raise
    
    def get_weaviate_collections(self) -> List[str]:
        """Get list of collections from Weaviate"""
        try:
            collections = list(self.weaviate_client.collections.list_all().keys())
            logger.info(f"Found {len(collections)} collections in Weaviate: {collections}")
            return collections
        except Exception as e:
            logger.error(f"Failed to get Weaviate collections: {str(e)}")
            raise
    
    def backup_to_gcs(self, collection_name: str, schema: CollectionSchema) -> List[str]:
        """Backup Weaviate collection data to GCS using RemoteBulkWriter"""
        logger.info(f"Starting GCS backup for collection: {collection_name}")
        
        try:
            # Connections parameters to access the remote GCS bucket
            conn = RemoteBulkWriter.GcsConnectParam(
                bucket_name=self.config.gcs_bucket_name,
                json_key_path=self.config.gcs_key_path,
            )
            
            with RemoteBulkWriter(
                schema=schema,
                remote_path=f"{self.config.gcs_remote_path}/{collection_name}",
                connect_param=conn,
                segment_size=self.config.segment_size,
                file_type=self.config.file_type,
            ) as remote_writer:
                
                logger.info('bulk writer created.')
                
                # Get Weaviate collection
                weaviate_collection = self.weaviate_client.collections.get(name=collection_name)
                
                # Iterate through Weaviate data and write to GCS
                total_rows = 0
                for item in weaviate_collection.iterator(include_vector=True):
                    # Transform data to match Milvus schema
                    row_data = self._transform_weaviate_item(item, schema)
                    remote_writer.append_row(row_data)
                    total_rows += 1
                    
                    if total_rows % 1000 == 0:
                        logger.info(f"Processed {total_rows} rows for {collection_name}")
                
                logger.info(f"Total rows processed: {total_rows}")
                logger.info(f"Rows in buffer: {remote_writer.buffer_row_count}")
                
                # Commit data to GCS
                remote_writer.commit()
                
                batch_files = remote_writer.batch_files
                logger.info(f"Successfully backed up {collection_name} to GCS. Files: {batch_files}")
                
                self.migration_stats['total_documents'] += total_rows
                return batch_files
                
        except Exception as e:
            logger.error(f"Failed to backup {collection_name} to GCS: {str(e)}")
            raise
    
    def _transform_weaviate_item(self, item, schema: CollectionSchema) -> Dict[str, Any]:
        """Transform Weaviate item to match Milvus schema"""
        row_data = {}
        
        # Get field definitions from schema
        field_names = [field.name for field in schema.fields]
        
        for field in schema.fields:
            if field.name == "id":
                # Use Weaviate UUID or generate one
                row_data["id"] = str(item.uuid) if hasattr(item, 'uuid') else str(item.properties.get('id', ''))
            elif field.name == "vector":
                # Use Weaviate vector
                row_data["vector"] = item.vector if item.vector else []
            else:
                # Map other properties
                row_data[field.name] = item.properties.get(field.name, None)
        
        return row_data
    
    def import_from_gcs(self, collection_name: str, gcs_files: List[str]) -> str:
        """Import data from GCS to Zilliz Cloud"""
        logger.info(f"Starting import from GCS for collection: {collection_name}")
        
        try:
            # Create import job
            job_id = bulk_import(
                url=self.config.zilliz_uri,
                token=self.config.zilliz_token,
                collection_name=collection_name,
                files=gcs_files
            )
            
            logger.info(f"Import job created with ID: {job_id}")
            
            # Monitor import progress
            while True:
                progress = get_import_progress(
                    url=self.config.zilliz_uri,
                    token=self.config.zilliz_token,
                    job_id=job_id
                )
                
                state = progress.get('state', 'Unknown')
                progress_percent = progress.get('progress', 0)
                
                logger.info(f"Import progress for {collection_name}: {state} ({progress_percent}%)")
                
                if state in ['Completed', 'Failed']:
                    break
                    
                time.sleep(10)  # Wait 10 seconds before checking again
            
            if state == 'Completed':
                logger.info(f"Successfully imported {collection_name} from GCS")
                self.migration_stats['migrated_documents'] += progress.get('importedRows', 0)
                return job_id
            else:
                error_msg = progress.get('reason', 'Unknown error')
                raise Exception(f"Import failed: {error_msg}")
                
        except Exception as e:
            logger.error(f"Failed to import {collection_name} from GCS: {str(e)}")
            raise
    
    def migrate_collection(self, collection_name: str) -> bool:
        """Migrate a single collection via GCS"""
        logger.info(f"Starting migration for collection: {collection_name}")
        
        try:
            # Step 1: Get or create collection schema
            safe_name = create_safe_collection_name(collection_name)
            schema = self.get_or_create_collection_schema(safe_name)
            
            # Step 2: Backup to GCS
            gcs_files = self.backup_to_gcs(collection_name, schema)
            self.migration_stats['gcs_files'].extend(gcs_files)
            
            # Step 3: Import from GCS
            job_id = self.import_from_gcs(safe_name, gcs_files)
            
            self.migration_stats['successful_collections'].append({
                'name': collection_name,
                'safe_name': safe_name,
                'job_id': job_id,
                'gcs_files': gcs_files
            })
            
            logger.info(f"Successfully migrated collection: {collection_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to migrate collection {collection_name}: {str(e)}")
            self.migration_stats['failed_collections'].append({
                'name': collection_name,
                'error': str(e)
            })
            return False
    
    def run_migration(self, collections: List[str] = None) -> Dict[str, Any]:
        """Run the complete GCS-based migration process"""
        self.migration_stats['start_time'] = datetime.now()
        logger.info("Starting GCS-based migration process")
        
        try:
            # Connect to both systems
            self.connect_weaviate()
            self.create_zilliz_connection()
            
            # Get collections to migrate
            if not collections:
                collections = self.get_weaviate_collections()
            
            self.migration_stats['total_collections'] = len(collections)
            logger.info(f"Migrating {len(collections)} collections: {collections}")
            
            # Migrate each collection
            for collection_name in collections:
                logger.info(f"Processing collection: {collection_name}")
                success = self.migrate_collection(collection_name)
                
                if not success:
                    logger.warning(f"Failed to migrate collection: {collection_name}")
            
            self.migration_stats['end_time'] = datetime.now()
            duration = self.migration_stats['end_time'] - self.migration_stats['start_time']
            
            # Log final statistics
            logger.info("="*60)
            logger.info("MIGRATION COMPLETED")
            logger.info("="*60)
            logger.info(f"Duration: {duration}")
            logger.info(f"Total collections: {self.migration_stats['total_collections']}")
            logger.info(f"Successful: {len(self.migration_stats['successful_collections'])}")
            logger.info(f"Failed: {len(self.migration_stats['failed_collections'])}")
            logger.info(f"Total documents processed: {self.migration_stats['total_documents']}")
            logger.info(f"Documents migrated: {self.migration_stats['migrated_documents']}")
            logger.info(f"GCS files created: {len(self.migration_stats['gcs_files'])}")
            
            return self.migration_stats
            
        except Exception as e:
            logger.error(f"Migration process failed: {str(e)}")
            raise
        finally:
            # Cleanup connections
            if self.weaviate_client:
                self.weaviate_client.close()
            if self.zilliz_connected:
                connections.disconnect("default")


def setup_logging(log_level: str = 'INFO'):
    """Setup logging configuration"""
    os.makedirs('logs', exist_ok=True)
    
    file_handler = logging.FileHandler(f'logs/gcs_migration_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    stream_handler = logging.StreamHandler(sys.stdout)
    
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[file_handler, stream_handler]
    )


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Migrate vector data from Weaviate to Zilliz Cloud via GCS',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python migrate_v2_gcs.py                           # Migrate all collections
  python migrate_v2_gcs.py -c Collection1 Collection2  # Migrate specific collections
  python migrate_v2_gcs.py --log-level DEBUG        # Enable debug logging
        """
    )
    
    parser.add_argument(
        '-c', '--collections',
        nargs='+',
        help='Specific collections to migrate (default: all collections)'
    )
    
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help='Set logging level (default: INFO)'
    )
    
    parser.add_argument(
        '--gcs-bucket',
        help='Override GCS bucket name'
    )
    
    parser.add_argument(
        '--gcs-key-path',
        help='Override GCS service account key path'
    )
    
    parser.add_argument(
        '--file-type',
        choices=['PARQUET', 'JSON'],
        default='PARQUET',
        help='File format for GCS backup (default: PARQUET)'
    )
    
    return parser.parse_args()


def main():
    """Main entry point"""
    args = parse_arguments()
    
    # Setup logging
    setup_logging(args.log_level)
    
    logger.info("Starting Weaviate to Zilliz Cloud GCS Migration Tool (v2)")
    logger.info(f"Arguments: {vars(args)}")
    
    try:
        # Create configuration
        config = GCSMigrationConfig()
        
        # Override with command line arguments
        if args.gcs_bucket:
            config.gcs_bucket_name = args.gcs_bucket
        if args.gcs_key_path:
            config.gcs_key_path = args.gcs_key_path
        if args.file_type:
            config.file_type = BulkFileType.PARQUET if args.file_type == 'PARQUET' else BulkFileType.JSON
        
        # Validate configuration
        config.validate()
        
        # Create and run migrator
        migrator = WeaviateToZillizGCSMigrator(config)
        stats = migrator.run_migration(args.collections)
        
        logger.info("GCS migration completed successfully!")
        
        # Save migration report
        report_file = f'logs/gcs_migration_report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
        with open(report_file, 'w') as f:
            # Convert datetime objects to strings for JSON serialization
            stats_copy = stats.copy()
            if stats_copy['start_time']:
                stats_copy['start_time'] = stats_copy['start_time'].isoformat()
            if stats_copy['end_time']:
                stats_copy['end_time'] = stats_copy['end_time'].isoformat()
            json.dump(stats_copy, f, indent=2)
        
        logger.info(f"Migration report saved to: {report_file}")
        
    except KeyboardInterrupt:
        logger.info("Migration interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Migration failed: {str(e)}")
        logger.debug("Full error details:", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()