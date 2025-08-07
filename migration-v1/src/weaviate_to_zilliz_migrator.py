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
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
import weaviate
from pymilvus import MilvusClient, DataType, CollectionSchema, FieldSchema
from dotenv import load_dotenv
import time
from tqdm import tqdm
import numpy as np
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from queue import Queue
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
        self.weaviate_endpoint = os.getenv('WEAVIATE_ENDPOINT', 'http://10.148.0.19')
        self.weaviate_api_key = os.getenv('WEAVIATE_API_KEY', 'WVF5YThaHlkYwhGUSmCRgsX3tD5ngdN8pkih')
        
        # Zilliz Cloud configuration
        self.zilliz_uri = os.getenv('ZILLIZ_CLOUD_URI', 'https://in01-a2db19e83930938.aws-us-west-2.vectordb.zillizcloud.com:19542')
        self.zilliz_token = os.getenv('ZILLIZ_CLOUD_API_KEY', '1f7f189111d65b5a28fa06dff3271af8453e682f3ac72449d9b5aec521d090645acc773cd8269521a38dabc5ae11fbe5e8fa48a1')
        self.zilliz_db_name = os.getenv('ZILLIZ_CLOUD_DATABASE', 'default')
        
        # Migration configuration
        self.batch_size = int(os.getenv('MIGRATION_BATCH_SIZE', '300'))
        self.max_retries = int(os.getenv('MIGRATION_MAX_RETRIES', '3'))
        self.retry_delay = float(os.getenv('MIGRATION_RETRY_DELAY', '1.0'))
        self.dimension = None
        
        # Threading configuration
        self.max_collection_workers = int(os.getenv('MAX_COLLECTION_WORKERS', '3'))
        self.thread_pool_executor = None
        
        # Thread-safe locks and queues
        self.stats_lock = threading.Lock()
        self.log_lock = threading.Lock()
        self.progress_lock = threading.Lock()
        self.batch_queue = Queue()
        
        # Client instances
        self.weaviate_client = None
        self.zilliz_client = None
        
        # Data transformer
        self.transformer = DataTransformer()
        
        # Migration statistics (thread-safe)
        self.migration_stats = {
            'start_time': None,
            'end_time': None,
            'total_collections': 0,
            'successful_collections': [],
            'failed_collections': [],
            'skipped_collections': [],
            'total_documents': 0,
            'migrated_documents': 0,
            'active_threads': 0
        }
        
        # Progress tracking
        self.collection_progress = {}
        self.global_progress_bar = None
        
    def thread_safe_log(self, level: str, message: str, thread_id: str = None):
        """Thread-safe logging with thread identification"""
        with self.log_lock:
            thread_info = f"[Thread-{thread_id or threading.current_thread().name}] " if thread_id or threading.current_thread().name != 'MainThread' else ""
            if level == 'info':
                logger.info(f"{thread_info}{message}")
            elif level == 'warning':
                logger.warning(f"{thread_info}{message}")
            elif level == 'error':
                logger.error(f"{thread_info}{message}")
            elif level == 'debug':
                logger.debug(f"{thread_info}{message}")
                
    def update_migration_stats(self, **kwargs):
        """Thread-safe statistics update"""
        with self.stats_lock:
            for key, value in kwargs.items():
                if key in self.migration_stats:
                    if isinstance(self.migration_stats[key], list):
                        if isinstance(value, list):
                            self.migration_stats[key].extend(value)
                        else:
                            self.migration_stats[key].append(value)
                    elif isinstance(self.migration_stats[key], (int, float)):
                        self.migration_stats[key] += value
                    else:
                        self.migration_stats[key] = value
                        
    def get_thread_safe_zilliz_client(self):
        """Get a thread-local Zilliz client to avoid connection conflicts"""
        # Each thread should have its own client instance
        if not hasattr(threading.current_thread(), 'zilliz_client'):
            threading.current_thread().zilliz_client = MilvusClient(
                uri=self.zilliz_uri,
                token=self.zilliz_token,
                db_name=self.zilliz_db_name
            )
        return threading.current_thread().zilliz_client
        
    def connect_weaviate(self):
        """Establish connection to Weaviate using v3 client"""
        try:
            # Configure authentication for v3 client
            if self.weaviate_api_key:
                auth_config = weaviate.AuthApiKey(api_key=self.weaviate_api_key)
                self.weaviate_client = weaviate.Client(
                    url=self.weaviate_endpoint,
                    auth_client_secret=auth_config,
                    timeout_config=(60, 60)  # (connect_timeout, read_timeout)
                )
            else:
                self.weaviate_client = weaviate.Client(
                    url=self.weaviate_endpoint,
                    timeout_config=(60, 60)
                )
            
            # Test connection
            if self.weaviate_client.is_ready():
                logger.info(f"Successfully connected to Weaviate at {self.weaviate_endpoint}")
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
            
    def create_zilliz_collection(self, collection_name: str, dimension: int, schema_info: Dict[str, Any] = None) -> bool:
        """Create a collection in Zilliz Cloud with the same schema"""
        try:
            # Ensure collection name is safe
            safe_collection_name = create_safe_collection_name(collection_name)
            if safe_collection_name != collection_name:
                logger.info(f"Collection name sanitized: {collection_name} -> {safe_collection_name}")
                collection_name = safe_collection_name
            
            # Check if collection already exists
            if self.zilliz_client.has_collection(collection_name):
                logger.info(f"Collection {collection_name} already exists in Zilliz Cloud, skipping creation")
                return False  # Collection already exists, skipped
                
            # Use transformer to create schema fields
            fields = self.transformer.create_zilliz_schema_fields(schema_info or {}, dimension)
            
            schema = CollectionSchema(fields=fields, description=f"Migrated from Weaviate: {collection_name}")
            
            # Create collection
            self.zilliz_client.create_collection(
                collection_name=collection_name,
                schema=schema,
                consistency_level="Strong"
            )
            
            # Create index on vector fields using prepare_index_params
            index_params = self.zilliz_client.prepare_index_params()
            
            # Index for dense vector field
            index_params.add_index(
                field_name="vector",
                index_type="HNSW",
                metric_type="IP",
                params={"M": 16, "efConstruction": 64}
            )
            
            # Index for sparse vector field
            index_params.add_index(
                field_name="sparse_vector",
                index_type="SPARSE_INVERTED_INDEX",
                metric_type="IP"
            )
            
            self.zilliz_client.create_index(
                collection_name=collection_name,
                index_params=index_params
            )
            
            # Load collection after creation
            self.load_collection(collection_name)
            
            logger.info(f"Successfully created and loaded collection {collection_name} in Zilliz Cloud")
            return True  # Collection created successfully
            
        except Exception as e:
            logger.error(f"Failed to create collection {collection_name}: {str(e)}")
            raise
    
    def load_collection(self, collection_name: str):
        """Load collection in Zilliz Cloud using REST API"""
        try:
            # Extract endpoint from URI
            endpoint = self.zilliz_uri

            url = f"{endpoint}/v2/vectordb/collections/load"
            
            headers = {
                "Authorization": f"Bearer {self.zilliz_token}",
                "Content-Type": "application/json"
            }
            
            data = {
                "collectionName": collection_name,
                "dbName": self.zilliz_db_name
            }
            
            response = requests.post(url, headers=headers, json=data, timeout=30)
            
            if response.status_code == 200:
                logger.info(f"Successfully loaded collection {collection_name}")
            else:
                logger.warning(f"Failed to load collection {collection_name}: {response.status_code} - {response.text}")
                
        except Exception as e:
            logger.warning(f"Failed to load collection {collection_name}: {str(e)}")
            # Don't raise exception as this is not critical for migration

    def get_zilliz_collections(self) -> List[str]:
        """Get all collection names from Zilliz Cloud using REST API"""
        try:
            # Extract endpoint from URI
            endpoint = self.zilliz_uri
            
            url = f"{endpoint}/v2/vectordb/collections/list"
            
            headers = {
                "Authorization": f"Bearer {self.zilliz_token}",
                "Content-Type": "application/json"
            }
            
            data = {
                "dbName": "default" if self.zilliz_db_name == "default" else self.zilliz_db_name
            }
            
            response = requests.post(url, headers=headers, json=data, timeout=30)
            
            if response.status_code == 200:
                result = response.json()
                collections = []
                if result.get('code') == 0 and 'data' in result:
                    collections = result['data']
                    logger.info(f"Found {len(collections)} collections in Zilliz Cloud: {collections}")
                else:
                    logger.warning(f"Unexpected response format: {result}")
                return collections
            else:
                logger.error(f"Failed to list collections: {response.status_code} - {response.text}")
                return []
                
        except Exception as e:
            logger.error(f"Failed to get Zilliz collections: {str(e)}")
            return []

    def load_all_collections(self):
        """Load all collections in Zilliz Cloud"""
        logger.info("Loading all collections in Zilliz Cloud")
        
        try:
            # Get all collections
            collections = self.get_zilliz_collections()
            
            if not collections:
                logger.warning("No collections found in Zilliz Cloud")
                return
            
            logger.info(f"Found {len(collections)} collections to load")
            
            # Load each collection
            successful_loads = []
            failed_loads = []
            
            for collection in collections:
                try:
                    logger.info(f"Loading collection: {collection}")
                    self.load_collection(collection)
                    successful_loads.append(collection)
                except Exception as e:
                    logger.error(f"Failed to load collection {collection}: {str(e)}")
                    failed_loads.append(collection)
            
            # Print summary
            logger.info(f"\nLoad Summary:")
            logger.info(f"Total collections: {len(collections)}")
            logger.info(f"Successfully loaded: {len(successful_loads)}")
            logger.info(f"Failed to load: {len(failed_loads)}")
            
            if successful_loads:
                logger.info(f"Successfully loaded collections:")
                for collection in successful_loads:
                    logger.info(f"  ✓ {collection}")
            
            if failed_loads:
                logger.warning(f"Failed to load collections:")
                for collection in failed_loads:
                    logger.warning(f"  ✗ {collection}")
                    
        except Exception as e:
            logger.error(f"Failed to load all collections: {str(e)}")
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
            
    def process_collection_data(self, collection_name: str, weaviate_data: List[Dict[str, Any]], 
                              schema_info: Dict[str, Any]) -> Tuple[int, bool]:
        """Process all data for a collection in a single operation"""
        thread_id = f"{collection_name}-upload"
        try:
            total_docs = len(weaviate_data)
            self.thread_safe_log('info', f"Processing collection {collection_name} with {total_docs} documents", thread_id)
            
            # Get thread-local Zilliz client
            zilliz_client = self.get_thread_safe_zilliz_client()
            
            # Transform all data for Zilliz
            zilliz_data = self.transform_data_for_zilliz(weaviate_data, schema_info)
            
            if not zilliz_data:
                self.thread_safe_log('warning', f"No valid data in collection {collection_name}", thread_id)
                return 0, False
            
            # Insert all data into Zilliz in one operation
            zilliz_client.insert(
                collection_name=collection_name,
                data=zilliz_data
            )
            
            migrated_count = len(zilliz_data)
            self.thread_safe_log('info', f"Successfully inserted {migrated_count} documents to collection {collection_name}", thread_id)
            
            # Update statistics
            self.update_migration_stats(migrated_documents=migrated_count)
            
            return migrated_count, True
            
        except Exception as e:
            self.thread_safe_log('error', f"Failed to process collection {collection_name}: {str(e)}", thread_id)
            return 0, False
            
    def migrate_collection_data(self, collection_name: str, weaviate_data: List[Dict[str, Any]], 
                              schema_info: Dict[str, Any]) -> int:
        """Migrate collection data in a single operation"""
        total_docs = len(weaviate_data)
        
        self.thread_safe_log('info', f"Starting single-thread migration for {collection_name} ({total_docs} documents)")
        
        # Process all data in one operation
        migrated_count, success = self.process_collection_data(collection_name, weaviate_data, schema_info)
        
        if success:
            self.thread_safe_log('info', f"Completed migration for {collection_name}: {migrated_count}/{total_docs} documents")
        else:
            self.thread_safe_log('error', f"Failed migration for {collection_name}")
            
        return migrated_count
            
    def migrate_collection(self, collection_name: str, limit: int = None):
        """Migrate a single collection from Weaviate to Zilliz Cloud"""
        logger.info(f"Starting migration for collection: {collection_name}")
        
        try:
            # check if collection exists in zilliz
            if self.zilliz_client.has_collection(collection_name):
                logger.warning(f"Collection {collection_name} already exists in Zilliz Cloud, skipping migration")
                return 0, True  # Return migrated count and skip status
            
            # Get collection schema
            schema_info = self.get_collection_schema(collection_name)
            
            # Get all data from Weaviate (with optional limit)
            weaviate_data = self.get_collection_data(collection_name, limit=limit)
            
            if not weaviate_data:
                logger.warning(f"No data found in collection {collection_name}")
                return 0, False  # Return migrated count and skip status
                
            # Extract dimension from first vector
            first_vector = weaviate_data[0]['_additional'].get('vector')
            if not first_vector:
                logger.error(f"No vector found in collection {collection_name}")
                return 0, False
                
            dimension = len(first_vector)
            logger.info(f"Vector dimension: {dimension}")
            
            # Create collection in Zilliz (returns True if created, False if skipped)
            collection_created = self.create_zilliz_collection(collection_name, dimension, schema_info)
            
            if not collection_created:
                logger.info(f"Collection {collection_name} already exists, skipping data migration")
                return 0, True  # Return 0 migrated docs and True for skipped
            
            # Log memory usage before migration
            log_memory_usage()
            
            # Use single-thread processing for complete data integrity
            total_docs = len(weaviate_data)
            migrated_count = self.migrate_collection_data(collection_name, weaviate_data, schema_info)
                        
            logger.info(f"Successfully migrated {migrated_count}/{total_docs} documents in {collection_name}")
            return migrated_count, False  # Return migrated count and not skipped
            
        except Exception as e:
            logger.error(f"Failed to migrate collection {collection_name}: {str(e)}")
            raise
            
    def verify_migration(self, collection_name: str) -> bool:
        """Verify the migration by comparing document counts"""
        try:
            # Get Weaviate count
            weaviate_data = self.get_collection_data(collection_name)
            weaviate_count = len(weaviate_data)
            
            # Get Zilliz count using get_collection_stats
            try:
                stats = self.zilliz_client.get_collection_stats(collection_name)
                # print the stats, this is a dict
                logger.info(f"Zilliz stats: {stats}")
                zilliz_count = stats.get('rowCount', 0)
            except:
                # Fallback: query all documents and count them
                result = self.zilliz_client.query(
                    collection_name=collection_name,
                    filter="",
                    output_fields=["id"],
                    limit=16384  # Max limit for Milvus
                )
                zilliz_count = len(result) if result else 0
            
            logger.info(f"Verification for {collection_name}:")
            logger.info(f"  Weaviate documents: {weaviate_count}")
            logger.info(f"  Zilliz documents: {zilliz_count}")
            
            if weaviate_count == zilliz_count:
                logger.info(f"✓ Migration verified successfully for {collection_name}")
                return True
            else:
                logger.warning(f"✗ Document count mismatch for {collection_name}")
                return True
                
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
                    'weaviate_endpoint': self.weaviate_endpoint,
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
            
    def migrate_single_collection_thread(self, collection_name: str, limit: int = None) -> Tuple[str, int, bool, bool]:
        """Migrate a single collection in a separate thread"""
        thread_id = f"collection-{collection_name}"
        try:
            self.thread_safe_log('info', f"Starting migration for collection: {collection_name}", thread_id)
            self.update_migration_stats(active_threads=1)
            
            migrated_docs, was_skipped = self.migrate_collection(collection_name, limit=limit)
            
            if was_skipped:
                self.thread_safe_log('info', f"Collection {collection_name} was skipped (already exists)", thread_id)
                return collection_name, migrated_docs, was_skipped, True
            else:
                # Verify migration
                verification_success = self.verify_migration(collection_name)
                self.thread_safe_log('info', f"Collection {collection_name} migration completed: {migrated_docs} documents", thread_id)
                return collection_name, migrated_docs, was_skipped, verification_success
                
        except Exception as e:
            self.thread_safe_log('error', f"Migration failed for {collection_name}: {str(e)}", thread_id)
            return collection_name, 0, False, False
        finally:
            self.update_migration_stats(active_threads=-1)
            
    def run_concurrent_migration(self, collections: List[str], limit: int = None):
        """Run migration with concurrent collection processing"""
        self.thread_safe_log('info', f"Starting concurrent migration for {len(collections)} collections")
        self.thread_safe_log('info', f"Configuration: {self.max_collection_workers} collection workers")
        
        # Use ThreadPoolExecutor for collection-level concurrency
        with ThreadPoolExecutor(max_workers=self.max_collection_workers, thread_name_prefix="collection") as executor:
            # Submit all collection migration tasks
            future_to_collection = {
                executor.submit(self.migrate_single_collection_thread, collection, limit): collection
                for collection in collections
            }
            
            # Track progress with overall progress bar
            with tqdm(total=len(collections), desc="Migrating collections", position=0) as collection_pbar:
                for future in as_completed(future_to_collection):
                    collection_name = future_to_collection[future]
                    try:
                        collection, migrated_docs, was_skipped, verification_success = future.result()
                        
                        if was_skipped:
                            self.update_migration_stats(skipped_collections=collection)
                        elif verification_success:
                            self.update_migration_stats(successful_collections=collection)
                        else:
                            self.update_migration_stats(failed_collections=collection)
                            
                        collection_pbar.set_postfix({
                            'Current': collection,
                            'Docs': migrated_docs,
                            'Status': 'Skipped' if was_skipped else ('Success' if verification_success else 'Failed')
                        })
                        
                    except Exception as e:
                        self.thread_safe_log('error', f"Collection {collection_name} raised exception: {str(e)}")
                        self.update_migration_stats(failed_collections=collection_name)
                    
                    collection_pbar.update(1)
            
    def run_migration(self, collections: Optional[List[str]] = None, limit: int = None, concurrent: bool = True):
        """Run the complete migration process"""
        mode = "concurrent" if concurrent else "sequential"
        logger.info(f"Starting Weaviate to Zilliz Cloud migration in {mode} mode")
        if limit:
            logger.info(f"Limiting migration to {limit} documents per collection")
        if concurrent:
            logger.info(f"Using {self.max_collection_workers} collection workers")
            
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
            
            if concurrent and len(collections) > 1:
                # Use concurrent migration for multiple collections
                self.run_concurrent_migration(collections, limit)
            else:
                # Use sequential migration (original logic)
                logger.info("Using sequential migration mode")
                for collection in collections:
                    try:
                        logger.info(f"\n{'='*60}")
                        logger.info(f"Processing collection: {collection}")
                        logger.info(f"{'='*60}")
                        
                        migrated_docs, was_skipped = self.migrate_collection(collection, limit=limit)
                        
                        if was_skipped:
                            self.migration_stats['skipped_collections'].append(collection)
                            logger.info(f"Collection {collection} was skipped (already exists)")
                        else:
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
        logger.info(f"Skipped: {len(stats['skipped_collections'])}")
        logger.info(f"Total documents migrated: {stats['migrated_documents']}")
        
        if stats['successful_collections']:
            logger.info(f"\nSuccessful collections:")
            for collection in stats['successful_collections']:
                logger.info(f"  ✓ {collection}")
                
        if stats['failed_collections']:
            logger.warning(f"\nFailed collections:")
            for collection in stats['failed_collections']:
                logger.warning(f"  ✗ {collection}")
                
        if stats['skipped_collections']:
            logger.info(f"\nSkipped collections (already exist):")
            for collection in stats['skipped_collections']:
                logger.info(f"  ⏭ {collection}")
                
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
    
    # Check if user wants to load all collections
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == 'load_collections':
        try:
            migrator.connect_zilliz()
            migrator.load_all_collections()
        except Exception as e:
            logger.error(f"Failed to load collections: {str(e)}")
            sys.exit(1)
        return
    
    # Check for command line arguments
    concurrent = True  # Default to concurrent mode
    if len(sys.argv) > 2 and sys.argv[2] == 'sequential':
        concurrent = False
        logger.info("Sequential mode requested via command line")
    
    # Run migration
    try:
        # You can specify specific collections to migrate
        # collections_to_migrate = ['Vector_index_abc123_Node']
        # migrator.run_migration(collections_to_migrate, concurrent=concurrent)
        
        # Or migrate all collections
        migrator.run_migration(concurrent=concurrent)
        
    except KeyboardInterrupt:
        logger.info("Migration interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Migration failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()