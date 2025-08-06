"""
Configuration management for Weaviate to Zilliz migration
"""

import os
from typing import Dict, Any
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


class MigrationConfig:
    """Configuration class for migration settings"""
    
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
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary"""
        return {
            'weaviate': {
                'endpoint': self.weaviate_endpoint,
                'has_api_key': bool(self.weaviate_api_key)
            },
            'zilliz': {
                'uri': self.zilliz_uri,
                'database': self.zilliz_db_name,
                'has_token': bool(self.zilliz_token)
            },
            'migration': {
                'batch_size': self.batch_size,
                'max_retries': self.max_retries,
                'retry_delay': self.retry_delay
            },
            'index': {
                'type': self.index_type,
                'metric_type': self.metric_type,
                'params': self.index_params
            }
        }