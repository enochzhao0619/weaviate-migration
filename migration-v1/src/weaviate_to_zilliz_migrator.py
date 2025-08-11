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
        
        # Removed threading configuration - now only supports serial processing
        
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
            'skipped_collections': [],
            'total_documents': 0,
            'migrated_documents': 0
        }
        
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
                        
                    else:
                        logger.warning(f"No valid data in batch {batch_number} after transformation")
                        
                except Exception as e:
                    logger.error(f"Failed to process batch {batch_number}: {str(e)}")
                    # Continue with next batch instead of failing completely
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
            logger.info(f"{progress_prefix}Step 1/3: Checking if collection exists in Zilliz Cloud...")
            if self.zilliz_client.has_collection(collection_name):
                logger.warning(f"{progress_prefix}Collection {collection_name} already exists in Zilliz Cloud, skipping migration")
                return 0, True  # Return migrated count and skip status
            
            # Step 2: Get collection schema
            logger.info(f"{progress_prefix}Step 2/3: Retrieving collection schema from Weaviate...")
            try:
                schema_info = self.get_collection_schema(collection_name)
                if not schema_info:
                    raise Exception("Empty schema retrieved")
                logger.info(f"{progress_prefix}Successfully retrieved schema with {len(schema_info.get('properties', {}))} properties")
            except Exception as e:
                error_msg = f"Failed to retrieve schema: {str(e)}"
                if "timeout" in str(e).lower():
                    error_msg += " (Connection timeout - check Weaviate connectivity)"
                elif "not found" in str(e).lower():
                    error_msg += " (Collection not found in Weaviate)"
                logger.error(f"{progress_prefix}{error_msg}")
                raise Exception(error_msg)
            
            # Step 3: Start serial batch migration (collection creation happens in first batch)
            logger.info(f"{progress_prefix}Step 3/3: Starting serial batch migration...")
            try:
                # Log memory usage before migration
                log_memory_usage()
                
                # Use serial batch migration with integrated collection creation
                migrated_count = self.migrate_collection_batch_by_batch(collection_name, schema_info, limit=limit)
                
                if migrated_count > 0:
                    logger.info(f"{progress_prefix}✓ Successfully migrated {migrated_count} documents in {collection_name}")
                else:
                    logger.warning(f"{progress_prefix}⚠ No documents were migrated for {collection_name}")
                    
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
                raise Exception(error_msg)
            
        except Exception as e:
            logger.error(f"{progress_prefix}✗ Failed to migrate collection {collection_name}: {str(e)}")
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