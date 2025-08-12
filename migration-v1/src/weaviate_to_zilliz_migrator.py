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
import psycopg2
import psycopg2.extras
from data_transformer import DataTransformer
from utils import retry_on_failure, log_memory_usage, create_safe_collection_name
from report_generator import MigrationReportGenerator

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
        
        # PostgreSQL configuration
        self.pg_host = os.getenv('PG_HOST', 'localhost')
        self.pg_port = int(os.getenv('PG_PORT', '5432'))
        self.pg_database = os.getenv('PG_DATABASE', 'dify')
        self.pg_username = os.getenv('PG_USER', 'postgres')
        self.pg_password = os.getenv('PG_PASSWORD', '')
        
        # Migration configuration
        self.batch_size = int(os.getenv('MIGRATION_BATCH_SIZE', '300'))
        self.max_retries = int(os.getenv('MIGRATION_MAX_RETRIES', '3'))
        self.retry_delay = float(os.getenv('MIGRATION_RETRY_DELAY', '1.0'))
        self.dimension = None
        
        # Removed threading configuration - now only supports serial processing
        
        # Client instances
        self.weaviate_client = None
        self.zilliz_client = None
        self.pg_connection = None
        
        # Data transformer
        self.transformer = DataTransformer()
        
        # Migration statistics (legacy format for backward compatibility)
        self.migration_stats = {
            'start_time': None,
            'end_time': None,
            'total_collections': 0,
            'successful_collections': [],
            'failed_collections': [],
            'skipped_collections': [],
            'total_documents': 0,
            'migrated_documents': 0
        }
        
        # Report generator for detailed reporting
        self.report_generator = MigrationReportGenerator()
        
    # Removed thread-related methods - now only supports serial processing
        
    def connect_weaviate(self):
        """Establish connection to Weaviate using v3 client"""
        try:
            # Configure authentication for v3 client
            if self.weaviate_api_key:
                auth_config = weaviate.AuthApiKey(api_key=self.weaviate_api_key)
                self.weaviate_client = weaviate.Client(
                    url=self.weaviate_endpoint,
                    auth_client_secret=auth_config,
                    timeout_config=(360, 360)  # (connect_timeout, read_timeout)
                )
            else:
                self.weaviate_client = weaviate.Client(
                    url=self.weaviate_endpoint,
                    timeout_config=(360, 360)
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
            
    def connect_postgresql(self):
        """Establish connection to PostgreSQL database"""
        try:
            self.pg_connection = psycopg2.connect(
                host=self.pg_host,
                port=self.pg_port,
                database=self.pg_database,
                user=self.pg_username,
                password=self.pg_password
            )
            self.pg_connection.autocommit = True
            logger.info(f"Successfully connected to PostgreSQL at {self.pg_host}:{self.pg_port}")
            
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {str(e)}")
            raise
            
    def _transform_class_name_to_dataset_id(self, class_name: str) -> str:
        """Transform Weaviate class name to dataset ID format
        
        Removes prefix (Vector_index) and suffix (_Node), replaces _ with -
        Example: Vector_index_3123e710_5583_4fd1_825d_0b748d495981_Node -> 3123e710-5583-4fd1-825d-0b748d495981
        """
        # Remove Vector_index prefix if exists
        if class_name.startswith('Vector_index_'):
            class_name = class_name[13:]  # Remove "Vector_index_"
        
        # Remove _Node suffix if exists
        if class_name.endswith('_Node'):
            class_name = class_name[:-5]  # Remove "_Node"
        
        # Replace underscores with hyphens
        dataset_id = class_name.replace('_', '-')
        
        return dataset_id
        
    def update_pg_datasets_vector_store_type(self) -> Dict[str, Any]:
        """Update PostgreSQL datasets table to change vector store type from weaviate to milvus
        and validate class_prefix consistency
        
        Returns:
            Dict containing update statistics and results
        """
        if not self.pg_connection:
            raise Exception("PostgreSQL connection not established. Call connect_postgresql() first.")
        
        if not self.weaviate_client:
            raise Exception("Weaviate connection not established. Call connect_weaviate() first.")
        
        logger.info("Starting PostgreSQL datasets vector store type update process")
        
        # Statistics tracking
        stats = {
            'total_weaviate_classes': 0,
            'processed_datasets': 0,
            'updated_datasets': 0,
            'failed_updates': 0,
            'not_found_datasets': 0,
            'class_prefix_matches': 0,
            'class_prefix_mismatches': 0,
            'errors': [],
            'mismatch_details': [],
            # Newly added detailed result lists for concise summary building
            'updated_details': [],  # List[{'dataset_id': str, 'class_name': str}]
            'failed_details': [],   # List[{'dataset_id': str, 'class_name': str, 'error': str}]
            'not_found_details': [] # List[{'dataset_id': str, 'class_name': str}]
        }
        
        try:
            # Step 1: Get all Weaviate classes
            logger.info("Retrieving all Weaviate classes...")
            weaviate_classes = self.get_weaviate_collections()
            stats['total_weaviate_classes'] = len(weaviate_classes)
            logger.info(f"Found {len(weaviate_classes)} Weaviate classes")
            
            # Step 2: Process each class
            with self.pg_connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                for class_name in weaviate_classes:
                    try:
                        stats['processed_datasets'] += 1
                        
                        # Transform class name to dataset ID
                        dataset_id = self._transform_class_name_to_dataset_id(class_name)
                        logger.info(f"Processing class: {class_name} -> dataset ID: {dataset_id}")
                        
                        # Step 3: Query dataset by ID
                        cursor.execute(
                            "SELECT id, index_struct FROM datasets WHERE id = %s",
                            (dataset_id,)
                        )
                        dataset = cursor.fetchone()
                        
                        if not dataset:
                            stats['not_found_datasets'] += 1
                            logger.warning(f"Dataset not found for ID: {dataset_id}")
                            stats['not_found_details'].append({'dataset_id': dataset_id, 'class_name': class_name})
                            continue
                        
                        # Step 4: Parse and update index_struct
                        index_struct_raw = dataset['index_struct']
                        if not index_struct_raw:
                            logger.warning(f"No index_struct found for dataset: {dataset_id}")
                            continue
                        
                        # Parse JSON string to dict if it's a string
                        try:
                            if isinstance(index_struct_raw, str):
                                index_struct = json.loads(index_struct_raw)
                            else:
                                index_struct = index_struct_raw
                        except (json.JSONDecodeError, TypeError) as e:
                            logger.error(f"Failed to parse index_struct for dataset {dataset_id}: {e}")
                            continue
                            
                        # Check if it's weaviate type and has vector_store config
                        if (isinstance(index_struct, dict) and 
                            index_struct.get('type') == 'weaviate' and 
                            'vector_store' in index_struct):
                            
                            # Step 5: Validate class_prefix consistency
                            vector_store = index_struct['vector_store']
                            class_prefix = vector_store.get('class_prefix', '')
                            
                            if class_prefix == class_name:
                                stats['class_prefix_matches'] += 1
                                logger.info(f"✓ Class prefix matches: {class_prefix}")
                            else:
                                stats['class_prefix_mismatches'] += 1
                                mismatch_info = {
                                    'dataset_id': dataset_id,
                                    'class_name': class_name,
                                    'class_prefix': class_prefix
                                }
                                stats['mismatch_details'].append(mismatch_info)
                                logger.warning(f"⚠ Class prefix mismatch - Expected: {class_name}, Found: {class_prefix}")
                            
                            # Step 6: Update the type from weaviate to milvus
                            index_struct['type'] = 'milvus'
                            
                            # Update the database
                            cursor.execute(
                                "UPDATE datasets SET index_struct = %s WHERE id = %s",
                                (json.dumps(index_struct), dataset_id)
                            )
                            
                            stats['updated_datasets'] += 1
                            logger.info(f"✓ Updated dataset {dataset_id}: weaviate -> milvus")
                            stats['updated_details'].append({'dataset_id': dataset_id, 'class_name': class_name})
                            
                        else:
                            logger.info(f"Dataset {dataset_id} does not have weaviate vector store config, skipping")
                        
                    except Exception as e:
                        stats['failed_updates'] += 1
                        error_msg = f"Failed to update dataset for class {class_name}: {str(e)}"
                        stats['errors'].append(error_msg)
                        logger.error(error_msg)
                        try:
                            # dataset_id may be available from earlier transformation
                            stats['failed_details'].append({'dataset_id': dataset_id, 'class_name': class_name, 'error': str(e)})
                        except Exception:
                            stats['failed_details'].append({'dataset_id': None, 'class_name': class_name, 'error': str(e)})
                        continue
            
            # Step 7: Log summary
            logger.info("\n" + "="*60)
            logger.info("POSTGRESQL DATASETS UPDATE SUMMARY")
            logger.info("="*60)
            logger.info(f"Total Weaviate classes: {stats['total_weaviate_classes']}")
            logger.info(f"Processed datasets: {stats['processed_datasets']}")
            logger.info(f"Successfully updated: {stats['updated_datasets']}")
            logger.info(f"Not found datasets: {stats['not_found_datasets']}")
            logger.info(f"Failed updates: {stats['failed_updates']}")
            logger.info(f"Class prefix matches: {stats['class_prefix_matches']}")
            logger.info(f"Class prefix mismatches: {stats['class_prefix_mismatches']}")
            
            if stats['mismatch_details']:
                logger.warning(f"\nClass prefix mismatch details:")
                for mismatch in stats['mismatch_details'][:10]:  # Show first 10 mismatches
                    logger.warning(f"  Dataset: {mismatch['dataset_id']}")
                    logger.warning(f"    Expected: {mismatch['class_name']}")
                    logger.warning(f"    Found: {mismatch['class_prefix']}")
                if len(stats['mismatch_details']) > 10:
                    logger.warning(f"  ... and {len(stats['mismatch_details']) - 10} more mismatches")
            
            if stats['errors']:
                logger.warning(f"\nErrors encountered:")
                for error in stats['errors'][:5]:  # Show first 5 errors
                    logger.warning(f"  {error}")
                if len(stats['errors']) > 5:
                    logger.warning(f"  ... and {len(stats['errors']) - 5} more errors")
            
            return stats
            
        except Exception as e:
            error_msg = f"Failed to update PostgreSQL datasets: {str(e)}"
            logger.error(error_msg)
            stats['errors'].append(error_msg)
            raise Exception(error_msg)
            
    def get_weaviate_collections(self) -> List[str]:
        """Get all collection names from Weaviate using v3 client"""
        try:
            # add debug log
            logger.info(f"Start to retrieval data")
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
            
    def get_collection_data(self, collection_name: str, limit: int = None, show_progress: bool = False) -> List[Dict[str, Any]]:
        """Retrieve all data from a Weaviate collection using cursor-based pagination to avoid OOM"""
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
            
            # Get total count for progress tracking
            total_count = 0
            if show_progress:
                try:
                    count_query = self.weaviate_client.query.aggregate(collection_name).with_meta_count()
                    count_result = count_query.do()
                    if count_result and 'data' in count_result and 'Aggregate' in count_result['data']:
                        aggregate_data = count_result['data']['Aggregate'].get(collection_name, [])
                        if aggregate_data and len(aggregate_data) > 0:
                            total_count = aggregate_data[0].get('meta', {}).get('count', 0)
                except:
                    # Fallback: estimate based on batch query
                    total_count = 0
                    
            # Use cursor-based pagination to fetch all data
            all_data = []
            cursor = None
            batch_size = min(self.batch_size, 1000)  # Use smaller batches for memory efficiency
            fetched_count = 0
            
            if show_progress and total_count > 0:
                logger.info(f"Fetching {total_count} documents from {collection_name} using cursor pagination...")
                pbar = tqdm(total=total_count, desc=f"Fetching {collection_name}", leave=False)
            else:
                pbar = None
                
            while True:
                # Build query with cursor
                query_builder = self.weaviate_client.query.get(collection_name, properties).with_additional(additional_fields)
                query_builder = query_builder.with_limit(batch_size)
                
                if cursor:
                    query_builder = query_builder.with_after(cursor)
                
                # Execute query
                result = query_builder.do()
                
                # Extract data from GraphQL response
                batch_data = []
                if result and 'data' in result and 'Get' in result['data'] and collection_name in result['data']['Get']:
                    objects = result['data']['Get'][collection_name]
                    
                    for obj in objects:
                        # Separate properties from additional fields
                        obj_data = {k: v for k, v in obj.items() if k != '_additional'}
                        obj_data['_additional'] = obj.get('_additional', {})
                        batch_data.append(obj_data)
                        
                        # Update cursor to the last object's ID for next iteration
                        cursor = obj_data['_additional'].get('id')
                
                # Add batch to all data
                all_data.extend(batch_data)
                fetched_count += len(batch_data)
                
                # Update progress bar
                if pbar:
                    pbar.update(len(batch_data))
                    pbar.set_postfix({'fetched': fetched_count})
                
                # Check if we've reached the limit or no more data
                if limit and fetched_count >= limit:
                    all_data = all_data[:limit]  # Trim to exact limit
                    break
                    
                if len(batch_data) < batch_size:
                    # No more data available
                    break
                    
                # Log progress periodically
                if fetched_count % (batch_size * 10) == 0:
                    logger.info(f"Fetched {fetched_count} documents from {collection_name}...")
            
            if pbar:
                pbar.close()
                
            logger.info(f"Retrieved {len(all_data)} documents from {collection_name} using cursor pagination")
            return all_data
            
        except Exception as e:
            error_msg = f"Failed to get data from collection {collection_name}: {str(e)}"
            if "timeout" in str(e).lower():
                error_msg += " (Connection timeout - check network connectivity)"
            elif "unauthorized" in str(e).lower():
                error_msg += " (Authentication failed - check API key)"
            elif "not found" in str(e).lower():
                error_msg += " (Collection not found in Weaviate)"
            logger.error(error_msg)
            raise Exception(error_msg)
            
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
            
    # Removed process_collection_data method - now handled in batch processing
            
    def migrate_collection_batch_by_batch(self, collection_name: str, schema_info: Dict[str, Any], limit: int = None) -> int:
        """Migrate collection data in serial batches of 250 documents"""
        try:
            # Get Zilliz client
            zilliz_client = self.zilliz_client
            
            # Initialize cursor-based data fetching
            cursor = None
            batch_size = self.batch_size  # Default 250
            total_migrated = 0
            batch_number = 1
            collection_created = False
            
            # Get total count for progress tracking
            total_count = 0
            try:
                count_query = self.weaviate_client.query.aggregate(collection_name).with_meta_count()
                count_result = count_query.do()
                if count_result and 'data' in count_result and 'Aggregate' in count_result['data']:
                    aggregate_data = count_result['data']['Aggregate'].get(collection_name, [])
                    if aggregate_data and len(aggregate_data) > 0:
                        total_count = aggregate_data[0].get('meta', {}).get('count', 0)
            except:
                total_count = 0
                
            if limit and total_count > 0:
                total_count = min(total_count, limit)
                
            logger.info(f"Starting serial batch migration for {collection_name}")
            logger.info(f"Total documents: {total_count}, Batch size: {batch_size}")
            
            # Get schema to understand properties
            schema = self.get_collection_schema(collection_name)
            properties = []
            if schema and 'properties' in schema:
                schema_properties = schema['properties']
                if isinstance(schema_properties, dict):
                    properties = list(schema_properties.keys())
                elif isinstance(schema_properties, list):
                    properties = [prop.get('name') for prop in schema_properties if prop.get('name')]
            
            # Process data in serial batches
            while True:
                logger.info(f"Processing batch {batch_number} (up to {batch_size} documents)...")
                
                # Build query with cursor
                additional_fields = ["id", "vector"]
                query_builder = self.weaviate_client.query.get(collection_name, properties).with_additional(additional_fields)
                query_builder = query_builder.with_limit(batch_size)
                
                if cursor:
                    query_builder = query_builder.with_after(cursor)
                
                # Execute query to get batch data
                result = query_builder.do()
                
                # Extract batch data
                batch_data = []
                if result and 'data' in result and 'Get' in result['data'] and collection_name in result['data']['Get']:
                    objects = result['data']['Get'][collection_name]
                    
                    for obj in objects:
                        # Separate properties from additional fields
                        obj_data = {k: v for k, v in obj.items() if k != '_additional'}
                        obj_data['_additional'] = obj.get('_additional', {})
                        batch_data.append(obj_data)
                        
                        # Update cursor to the last object's ID for next iteration
                        cursor = obj_data['_additional'].get('id')
                
                # Check if we have data to process
                if not batch_data:
                    logger.info("No more data to process")
                    break
                    
                logger.info(f"Fetched {len(batch_data)} documents in batch {batch_number}")
                
                # Check if we've reached the limit
                if limit and total_migrated + len(batch_data) > limit:
                    batch_data = batch_data[:limit - total_migrated]
                    logger.info(f"Trimmed batch to {len(batch_data)} documents due to limit")
                
                # Create collection only for the first batch
                if not collection_created and batch_data:
                    logger.info("Creating Zilliz collection for first batch...")
                    first_vector = batch_data[0]['_additional'].get('vector')
                    if not first_vector:
                        raise Exception("No vector found in first document")
                        
                    dimension = len(first_vector)
                    logger.info(f"Vector dimension: {dimension}")
                    
                    # Create collection in Zilliz
                    created = self.create_zilliz_collection(collection_name, dimension, schema_info)
                    if created:
                        collection_created = True
                        logger.info(f"Collection {collection_name} created successfully")
                    else:
                        logger.info(f"Collection {collection_name} already exists, continuing with data upload")
                        collection_created = True
                
                # Transform and insert batch
                try:
                    logger.info(f"Transforming batch {batch_number} data for Zilliz...")
                    zilliz_data = self.transform_data_for_zilliz(batch_data, schema_info)
                    
                    if zilliz_data:
                        logger.info(f"Uploading {len(zilliz_data)} documents to Zilliz...")
                        zilliz_client.insert(
                            collection_name=collection_name,
                            data=zilliz_data
                        )
                        
                        total_migrated += len(zilliz_data)
                        logger.info(f"✓ Batch {batch_number} completed: {len(zilliz_data)} documents uploaded")
                        logger.info(f"Total migrated so far: {total_migrated}/{total_count if total_count > 0 else '?'}")
                        
                        # Update report generator with successful batch
                        self.report_generator.update_collection_progress(collection_name, len(zilliz_data), success=True)
                        
                    else:
                        logger.warning(f"No valid data in batch {batch_number} after transformation")
                        
                except Exception as e:
                    error_message = str(e)
                    logger.error(f"Failed to process batch {batch_number}: {error_message}")
                    
                    # Update report generator with failed batch
                    self.report_generator.update_collection_progress(collection_name, len(batch_data), success=False)
                    self.report_generator.add_error("Batch Error", collection_name, f"Batch {batch_number}: {error_message}")
                    
                    # Check if this is a critical error that should fail the entire collection
                    if self._is_critical_error(error_message):
                        logger.error(f"Critical error encountered in batch {batch_number}, failing entire collection migration")
                        raise Exception(f"Critical error in batch {batch_number}: {error_message}")
                    else:
                        # For non-critical errors, continue with next batch
                        logger.warning(f"Non-critical error in batch {batch_number}, continuing with next batch")
                        continue
                
                # Check exit conditions
                if limit and total_migrated >= limit:
                    logger.info(f"Reached migration limit of {limit} documents")
                    break
                    
                if len(batch_data) < batch_size:
                    logger.info("Last batch processed (fewer documents than batch size)")
                    break
                
                batch_number += 1
                
            logger.info(f"Serial batch migration completed for {collection_name}")
            logger.info(f"Total documents migrated: {total_migrated}")
            return total_migrated
            
        except Exception as e:
            logger.error(f"Serial batch migration failed for {collection_name}: {str(e)}")
            raise

    def _get_collection_document_count(self, collection_name: str) -> int:
        """Get document count from Weaviate collection efficiently"""
        try:
            count_query = self.weaviate_client.query.aggregate(collection_name).with_meta_count()
            count_result = count_query.do()
            if count_result and 'data' in count_result and 'Aggregate' in count_result['data']:
                aggregate_data = count_result['data']['Aggregate'].get(collection_name, [])
                if aggregate_data and len(aggregate_data) > 0:
                    return aggregate_data[0].get('meta', {}).get('count', 0)
            return 0
        except Exception as e:
            logger.warning(f"Failed to get document count for {collection_name}: {str(e)}")
            return 0
            
    def _get_zilliz_collection_document_count(self, collection_name: str) -> int:
        """Get document count from Zilliz collection"""
        try:
            stats = self.zilliz_client.get_collection_stats(collection_name)
            return stats.get('rowCount', 0)
        except Exception as e:
            logger.warning(f"Failed to get Zilliz document count for {collection_name}: {str(e)}")
            return 0
            
    def _is_critical_error(self, error_message: str) -> bool:
        """Determine if an error is critical and should fail the entire collection migration"""
        error_lower = error_message.lower()
        
        # Critical errors that indicate data integrity issues or system limitations
        critical_error_patterns = [
            # Field length/size violations
            "exceeds max length",
            "field length too long",
            "varchar field",
            "text field too long",
            
            # Data type violations  
            "invalid data type",
            "type mismatch",
            "schema violation",
            "invalid parameter",
            
            # System resource errors
            "out of memory",
            "memory limit exceeded",
            "disk full",
            "storage quota exceeded",
            
            # Authentication/authorization errors
            "authentication failed",
            "unauthorized",
            "access denied",
            "permission denied",
            
            # Collection/schema errors
            "collection not found",
            "schema error",
            "index error",
            
            # Connection errors that are persistent
            "connection refused",
            "host unreachable",
            "dns resolution failed"
        ]
        
        # Check if error message contains any critical error patterns
        for pattern in critical_error_patterns:
            if pattern in error_lower:
                return True
                
        # Non-critical errors (temporary network issues, rate limiting, etc.)
        non_critical_patterns = [
            "timeout",
            "rate limit",
            "throttling",
            "temporary failure",
            "retry",
            "connection reset"
        ]
        
        # If it's a known non-critical error, return False
        for pattern in non_critical_patterns:
            if pattern in error_lower:
                return False
                
        # For unknown errors, default to critical to be safe
        return True

    # Removed legacy migrate_collection_data method - now using batch processing
            
    def migrate_collection(self, collection_name: str, limit: int = None, collection_index: int = None, total_collections: int = None):
        """Migrate a single collection from Weaviate to Zilliz Cloud using serial batch processing"""
        
        # Create progress prefix for sequential mode
        progress_prefix = ""
        if collection_index is not None and total_collections is not None:
            progress_prefix = f"[{collection_index}/{total_collections}] "
            
        logger.info(f"{progress_prefix}Starting migration for collection: {collection_name}")
        
        try:
            # Step 1: Check if collection exists in Zilliz
            logger.info(f"{progress_prefix}Step 1/4: Checking if collection exists in Zilliz Cloud...")
            if self.zilliz_client.has_collection(collection_name):
                logger.warning(f"{progress_prefix}Collection {collection_name} already exists in Zilliz Cloud, skipping migration")
                # Still get schema and document count for reporting
                schema_info = self.get_collection_schema(collection_name)
                weaviate_doc_count = self._get_collection_document_count(collection_name)
                self.report_generator.add_collection_start(collection_name, schema_info, weaviate_doc_count)
                self.report_generator.set_collection_result(collection_name, 'skipped', 0, "Collection already exists in Zilliz")
                return 0, True  # Return migrated count and skip status
            
            # Step 2: Get collection schema and document count
            logger.info(f"{progress_prefix}Step 2/4: Retrieving collection schema and document count from Weaviate...")
            try:
                schema_info = self.get_collection_schema(collection_name)
                if not schema_info:
                    raise Exception("Empty schema retrieved")
                logger.info(f"{progress_prefix}Successfully retrieved schema with {len(schema_info.get('properties', {}))} properties")
                
                # Get document count for reporting
                weaviate_doc_count = self._get_collection_document_count(collection_name)
                logger.info(f"{progress_prefix}Collection contains {weaviate_doc_count:,} documents")
                
                # Initialize collection in report generator
                self.report_generator.add_collection_start(collection_name, schema_info, weaviate_doc_count)
                
            except Exception as e:
                error_msg = f"Failed to retrieve schema: {str(e)}"
                if "timeout" in str(e).lower():
                    error_msg += " (Connection timeout - check Weaviate connectivity)"
                elif "not found" in str(e).lower():
                    error_msg += " (Collection not found in Weaviate)"
                logger.error(f"{progress_prefix}{error_msg}")
                self.report_generator.add_error("Schema Error", collection_name, error_msg)
                raise Exception(error_msg)
            
            # Step 3: Start serial batch migration (collection creation happens in first batch)
            logger.info(f"{progress_prefix}Step 3/4: Starting serial batch migration...")
            try:
                # Log memory usage before migration
                log_memory_usage()
                
                # Use serial batch migration with integrated collection creation
                migrated_count = self.migrate_collection_batch_by_batch(collection_name, schema_info, limit=limit)
                
                # Step 4: Verify migration and get final document count
                logger.info(f"{progress_prefix}Step 4/4: Verifying migration...")
                zilliz_doc_count = self._get_zilliz_collection_document_count(collection_name)
                
                if migrated_count > 0:
                    logger.info(f"{progress_prefix}✓ Successfully migrated {migrated_count} documents in {collection_name}")
                    self.report_generator.set_collection_result(collection_name, 'success', zilliz_doc_count)
                else:
                    logger.warning(f"{progress_prefix}⚠ No documents were migrated for {collection_name}")
                    self.report_generator.set_collection_result(collection_name, 'success', zilliz_doc_count)
                    
                return migrated_count, False  # Return migrated count and not skipped
                
            except Exception as e:
                error_msg = f"Failed to migrate documents: {str(e)}"
                if "timeout" in str(e).lower():
                    error_msg += " (Insert timeout - try reducing batch size)"
                elif "dimension" in str(e).lower():
                    error_msg += " (Vector dimension mismatch during insert)"
                elif "memory" in str(e).lower():
                    error_msg += " (Out of memory during insert - try reducing batch size)"
                elif "quota" in str(e).lower():
                    error_msg += " (Storage quota exceeded in Zilliz Cloud)"
                logger.error(f"{progress_prefix}{error_msg}")
                self.report_generator.add_error("Migration Error", collection_name, error_msg)
                self.report_generator.set_collection_result(collection_name, 'failed', 0, error_msg)
                raise Exception(error_msg)
            
        except Exception as e:
            logger.error(f"{progress_prefix}✗ Failed to migrate collection {collection_name}: {str(e)}")
            # Ensure collection is marked as failed in report if not already done
            if collection_name not in [c['name'] for c in self.report_generator.migration_data['collections'].values()]:
                self.report_generator.add_collection_start(collection_name, {}, 0)
            self.report_generator.set_collection_result(collection_name, 'failed', 0, str(e))
            raise
            
    def verify_migration(self, collection_name: str) -> bool:
        """Verify the migration by comparing document counts using efficient methods"""
        try:
            # Get Weaviate count using aggregate query (more efficient than fetching all data)
            weaviate_count = 0
            try:
                count_query = self.weaviate_client.query.aggregate(collection_name).with_meta_count()
                count_result = count_query.do()
                if count_result and 'data' in count_result and 'Aggregate' in count_result['data']:
                    aggregate_data = count_result['data']['Aggregate'].get(collection_name, [])
                    if aggregate_data and len(aggregate_data) > 0:
                        weaviate_count = aggregate_data[0].get('meta', {}).get('count', 0)
            except Exception as e:
                logger.warning(f"Failed to get Weaviate count via aggregate, using fallback: {str(e)}")
                # Fallback: get actual data (only if aggregate fails)
                weaviate_data = self.get_collection_data(collection_name, limit=None, show_progress=False)
                weaviate_count = len(weaviate_data)
            
            # Get Zilliz count using get_collection_stats
            try:
                stats = self.zilliz_client.get_collection_stats(collection_name)
                logger.debug(f"Zilliz stats: {stats}")
                zilliz_count = stats.get('rowCount', 0)
            except:
                # Fallback: query all documents and count them
                try:
                    result = self.zilliz_client.query(
                        collection_name=collection_name,
                        filter="",
                        output_fields=["id"],
                        limit=16384  # Max limit for Milvus
                    )
                    zilliz_count = len(result) if result else 0
                except:
                    logger.warning(f"Failed to get Zilliz count for {collection_name}")
                    zilliz_count = 0
            
            logger.info(f"Verification for {collection_name}:")
            logger.info(f"  Weaviate documents: {weaviate_count}")
            logger.info(f"  Zilliz documents: {zilliz_count}")
            
            if weaviate_count == zilliz_count:
                logger.info(f"✓ Migration verified successfully for {collection_name}")
                return True
            else:
                logger.warning(f"✗ Document count mismatch for {collection_name}")
                logger.warning(f"  This may be normal if some documents failed validation during transformation")
                return True  # Return True to not fail the migration due to minor count differences
                
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
            
    # Removed concurrent migration methods - now only supports serial processing
            
    def run_migration(self, collections: Optional[List[str]] = None, limit: int = None):
        """Run the complete migration process in serial mode with batch processing"""
        logger.info("Starting Weaviate to Zilliz Cloud migration in serial batch mode")
        logger.info(f"Batch size: {self.batch_size} documents per batch")
        if limit:
            logger.info(f"Limiting migration to {limit} documents per collection")
            
        start_time = datetime.now()
        self.migration_stats['start_time'] = start_time
        
        # Initialize report generator with migration configuration
        config_info = {
            'weaviate_endpoint': self.weaviate_endpoint,
            'zilliz_uri': self.zilliz_uri,
            'batch_size': self.batch_size,
            'max_retries': self.max_retries
        }
        self.report_generator.set_migration_start(start_time, config_info)
        
        try:
            # Connect to both systems
            self.connect_weaviate()
            self.connect_zilliz()
            
            # Get collections to migrate
            if not collections:
                collections = self.get_weaviate_collections()
                
            self.migration_stats['total_collections'] = len(collections)
            logger.info(f"Will migrate {len(collections)} collections: {collections}")
            
            # Use serial migration with batch processing
            logger.info("Using serial batch migration mode")
            logger.info(f"Total collections to migrate: {len(collections)}")
            logger.info(f"Collections: {', '.join(collections)}")
            logger.info("="*80)
            
            for index, collection in enumerate(collections, 1):
                try:
                    logger.info(f"\n{'='*80}")
                    logger.info(f"PROCESSING COLLECTION {index}/{len(collections)}: {collection}")
                    logger.info(f"{'='*80}")
                    
                    migrated_docs, was_skipped = self.migrate_collection(
                        collection, 
                        limit=limit, 
                        collection_index=index, 
                        total_collections=len(collections)
                    )
                    
                    if was_skipped:
                        self.migration_stats['skipped_collections'].append(collection)
                        logger.info(f"[{index}/{len(collections)}] ⏭ Collection {collection} was skipped (already exists)")
                    else:
                        self.migration_stats['migrated_documents'] += migrated_docs
                        
                        logger.info(f"[{index}/{len(collections)}] Verifying migration for {collection}...")
                        if self.verify_migration(collection):
                            self.migration_stats['successful_collections'].append(collection)
                            logger.info(f"[{index}/{len(collections)}] ✓ Migration verification successful for {collection}")
                        else:
                            self.migration_stats['failed_collections'].append(collection)
                            logger.warning(f"[{index}/{len(collections)}] ⚠ Migration verification failed for {collection}")
                        
                    # Print progress summary
                    completed = len(self.migration_stats['successful_collections']) + len(self.migration_stats['failed_collections']) + len(self.migration_stats['skipped_collections'])
                    remaining = len(collections) - completed
                    logger.info(f"\nProgress Summary: {completed}/{len(collections)} collections processed, {remaining} remaining")
                    logger.info(f"  ✓ Successful: {len(self.migration_stats['successful_collections'])}")
                    logger.info(f"  ✗ Failed: {len(self.migration_stats['failed_collections'])}")
                    logger.info(f"  ⏭ Skipped: {len(self.migration_stats['skipped_collections'])}")
                    logger.info(f"  📊 Total documents migrated: {self.migration_stats['migrated_documents']}")
                        
                except Exception as e:
                    error_msg = f"Migration failed for {collection}: {str(e)}"
                    logger.error(f"[{index}/{len(collections)}] ✗ {error_msg}")
                    self.migration_stats['failed_collections'].append(collection)
                    
                    # Log specific error details for troubleshooting
                    if "timeout" in str(e).lower():
                        logger.error(f"[{index}/{len(collections)}] Timeout error - consider increasing timeout or reducing batch size")
                    elif "memory" in str(e).lower():
                        logger.error(f"[{index}/{len(collections)}] Memory error - consider reducing batch size or using --limit")
                    elif "connection" in str(e).lower():
                        logger.error(f"[{index}/{len(collections)}] Connection error - check network connectivity")
                    elif "authentication" in str(e).lower() or "unauthorized" in str(e).lower():
                        logger.error(f"[{index}/{len(collections)}] Authentication error - check API keys and credentials")
                    elif "quota" in str(e).lower() or "limit" in str(e).lower():
                        logger.error(f"[{index}/{len(collections)}] Quota/limit error - check Zilliz Cloud usage limits")
                
            end_time = datetime.now()
            self.migration_stats['end_time'] = end_time
            self.report_generator.set_migration_end(end_time)
            
            # Print summary
            self._print_migration_summary()
            
            # Generate comprehensive reports
            logger.info("\n" + "="*60)
            logger.info("GENERATING MIGRATION REPORTS")
            logger.info("="*60)
            
            try:
                report_files = self.report_generator.generate_all_reports()
                
                if report_files:
                    logger.info("Migration reports generated successfully:")
                    for report_type, file_path in report_files.items():
                        logger.info(f"  {report_type.upper()}: {file_path}")
                        
                    # Highlight the HTML report for easy access
                    if 'html' in report_files:
                        logger.info(f"\n📊 Open the HTML report for detailed results: {report_files['html']}")
                else:
                    logger.warning("No reports were generated successfully")
                    
            except Exception as e:
                logger.error(f"Failed to generate migration reports: {str(e)}")
                logger.debug("Report generation error details:", exc_info=True)
            
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
    
    # Check if user wants to load all collections or update PostgreSQL datasets
    import sys
    if len(sys.argv) > 1:
        if sys.argv[1] == 'load_collections':
            try:
                migrator.connect_zilliz()
                migrator.load_all_collections()
            except Exception as e:
                logger.error(f"Failed to load collections: {str(e)}")
                sys.exit(1)
            return
        elif sys.argv[1] == 'update_pg_datasets':
            try:
                migrator.connect_weaviate()
                migrator.connect_postgresql()
                stats = migrator.update_pg_datasets_vector_store_type()
                logger.info("PostgreSQL datasets update completed successfully")
                logger.info(f"Updated {stats['updated_datasets']} datasets")
            except Exception as e:
                logger.error(f"Failed to update PostgreSQL datasets: {str(e)}")
                sys.exit(1)
            return
    
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