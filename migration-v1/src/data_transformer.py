"""
Data transformation utilities for Weaviate to Zilliz migration
"""

import json
import logging
from typing import Any, Dict, List, Optional, Tuple
from pymilvus import DataType, FieldSchema
from utils import (
    validate_vector, 
    sanitize_field_name, 
    truncate_text, 
    extract_text_content,
    safe_json_serialize
)

logger = logging.getLogger(__name__)


class DataTransformer:
    """Handles data transformation between Weaviate and Zilliz formats"""
    
    def __init__(self):
        self.field_mappings = {}
        self.type_mappings = {
            'text': DataType.VARCHAR,
            'string': DataType.VARCHAR,
            'int': DataType.INT64,
            'integer': DataType.INT64,
            'number': DataType.DOUBLE,
            'float': DataType.DOUBLE,
            'double': DataType.DOUBLE,
            'boolean': DataType.BOOL,
            'bool': DataType.BOOL
        }
        
    def analyze_weaviate_schema(self, schema: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze Weaviate schema to determine field types and mappings"""
        analysis = {
            'properties': {},
            'vector_properties': [],
            'text_properties': [],
            'numeric_properties': [],
            'boolean_properties': []
        }
        
        if 'properties' not in schema:
            return analysis
            
        properties = schema['properties']
        
        # Handle both dict and list formats for properties
        if isinstance(properties, dict):
            # Dict format: {prop_name: prop_info}
            for prop_name, prop_info in properties.items():
                self._process_property(prop_name, prop_info, analysis)
        elif isinstance(properties, list):
            # List format: [{name: prop_name, ...}, ...]
            for prop_info in properties:
                prop_name = prop_info.get('name')
                if prop_name:
                    self._process_property(prop_name, prop_info, analysis)
        else:
            logger.warning(f"Unknown properties format: {type(properties)}")
                
        return analysis
        
    def _process_property(self, prop_name: str, prop_info: Dict[str, Any], analysis: Dict[str, Any]):
        """Process a single property and add it to the analysis"""
        data_types = prop_info.get('dataType', ['text'])
        primary_type = data_types[0].lower() if data_types else 'text'
        
        # Sanitize field name
        safe_name = sanitize_field_name(prop_name)
        
        analysis['properties'][prop_name] = {
            'original_name': prop_name,
            'safe_name': safe_name,
            'data_type': primary_type,
            'zilliz_type': self.type_mappings.get(primary_type, DataType.VARCHAR)
        }
        
        # Categorize properties
        if primary_type in ['text', 'string']:
            analysis['text_properties'].append(prop_name)
        elif primary_type in ['int', 'integer', 'number', 'float', 'double']:
            analysis['numeric_properties'].append(prop_name)
        elif primary_type in ['boolean', 'bool']:
            analysis['boolean_properties'].append(prop_name)
        
    def create_zilliz_schema_fields(self, weaviate_schema: Dict[str, Any], vector_dim: int) -> List[FieldSchema]:
        """Create Zilliz schema fields compatible with Dify structure"""
        fields = []
        
        # Create fields compatible with Dify's Zilliz structure
        fields.extend([
            FieldSchema(name="id", dtype=DataType.VARCHAR, is_primary=True, max_length=65535),
            FieldSchema(name="page_content", dtype=DataType.VARCHAR, max_length=65535),
            FieldSchema(name="vector", dtype=DataType.FLOAT_VECTOR, dim=vector_dim),
            FieldSchema(name="sparse_vector", dtype=DataType.SPARSE_FLOAT_VECTOR),
            FieldSchema(name="metadata", dtype=DataType.JSON),
        ])
        
        logger.info(f"Created Dify-compatible schema with {len(fields)} fields: {[f.name for f in fields]}")
        return fields
        
    def transform_document(self, weaviate_doc: Dict[str, Any], schema_analysis: Dict[str, Any] = None) -> Optional[Dict[str, Any]]:
        """Transform a single Weaviate document to Dify-compatible Zilliz format"""
        try:
            # Extract basic components
            additional = weaviate_doc.get('_additional', {})
            doc_id = additional.get('id')
            vector = additional.get('vector')
            
            if not doc_id:
                logger.warning("Document missing ID, skipping")
                return None
                
            if not vector or not validate_vector(vector):
                logger.warning(f"Document {doc_id} has invalid vector, skipping")
                return None
                
            # Extract properties (everything except _additional)
            properties = {k: v for k, v in weaviate_doc.items() if k != '_additional'}
            
            # Extract text content for page_content field
            text_content = extract_text_content(properties)
            
            # Create metadata without text field to avoid duplication with page_content
            metadata = {k: v for k, v in properties.items() if k != 'text'}
            
            # Create Dify-compatible Zilliz document structure
            zilliz_doc = {
                'id': str(doc_id),
                'page_content': truncate_text(text_content),
                'vector': vector,
                'sparse_vector': {},  # Empty sparse vector as required
                'metadata': metadata  # Store properties in metadata JSON field (excluding text)
            }
            
            # Note: No longer extracting individual properties as separate fields
            # All data is stored in the metadata JSON field to match Dify structure
                            
            return zilliz_doc
            
        except Exception as e:
            doc_id = weaviate_doc.get('_additional', {}).get('id', 'unknown')
            logger.error(f"Failed to transform document {doc_id}: {str(e)}")
            return None
            
    def _transform_field_value(self, value: Any, target_type: DataType) -> Any:
        """Transform field value to match target Zilliz data type"""
        if value is None:
            return None
            
        try:
            if target_type == DataType.VARCHAR:
                return truncate_text(str(value))
            elif target_type == DataType.INT64:
                if isinstance(value, bool):
                    return int(value)
                return int(float(value))  # Handle string numbers
            elif target_type == DataType.DOUBLE:
                return float(value)
            elif target_type == DataType.BOOL:
                if isinstance(value, str):
                    return value.lower() in ('true', '1', 'yes', 'on')
                return bool(value)
            else:
                return str(value)
                
        except (ValueError, TypeError) as e:
            logger.warning(f"Failed to transform value {value} to {target_type}: {str(e)}")
            return None
            
    def transform_batch(self, weaviate_batch: List[Dict[str, Any]], schema_analysis: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """Transform a batch of Weaviate documents to Zilliz format"""
        transformed_docs = []
        
        for doc in weaviate_batch:
            transformed = self.transform_document(doc, schema_analysis)
            if transformed:
                transformed_docs.append(transformed)
                
        logger.debug(f"Transformed {len(transformed_docs)}/{len(weaviate_batch)} documents")
        return transformed_docs
        
    def validate_transformed_data(self, zilliz_docs: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[str]]:
        """Validate transformed data and return valid documents and error messages"""
        valid_docs = []
        errors = []
        
        for i, doc in enumerate(zilliz_docs):
            try:
                # Check required fields for Dify-compatible structure
                required_fields = ['id', 'page_content', 'vector', 'metadata']
                missing_fields = [field for field in required_fields if field not in doc]
                
                if missing_fields:
                    errors.append(f"Document {i}: Missing required fields: {missing_fields}")
                    continue
                    
                # Validate vector
                if not validate_vector(doc['vector']):
                    errors.append(f"Document {i}: Invalid vector data")
                    continue
                    
                # Validate field lengths
                if len(doc['id']) > 65535:
                    errors.append(f"Document {i}: ID too long")
                    continue
                    
                if len(doc['page_content']) > 65535:
                    doc['page_content'] = truncate_text(doc['page_content'])
                    
                # Validate metadata is JSON serializable
                try:
                    safe_json_serialize(doc['metadata'])
                except Exception:
                    errors.append(f"Document {i}: Metadata not JSON serializable")
                    continue
                    
                valid_docs.append(doc)
                
            except Exception as e:
                errors.append(f"Document {i}: Validation error: {str(e)}")
                
        return valid_docs, errors
        
    def get_field_statistics(self, weaviate_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Get statistics about fields in the Weaviate data"""
        stats = {
            'total_documents': len(weaviate_data),
            'field_frequency': {},
            'field_types': {},
            'vector_dimensions': set(),
            'sample_data': {}
        }
        
        for doc in weaviate_data[:100]:  # Sample first 100 documents
            # Analyze properties
            properties = {k: v for k, v in doc.items() if k != '_additional'}
            
            for field, value in properties.items():
                # Count field frequency
                stats['field_frequency'][field] = stats['field_frequency'].get(field, 0) + 1
                
                # Determine field type
                value_type = type(value).__name__
                if field not in stats['field_types']:
                    stats['field_types'][field] = {}
                stats['field_types'][field][value_type] = stats['field_types'][field].get(value_type, 0) + 1
                
                # Store sample values
                if field not in stats['sample_data']:
                    stats['sample_data'][field] = []
                if len(stats['sample_data'][field]) < 3:
                    stats['sample_data'][field].append(value)
                    
            # Analyze vector dimensions
            vector = doc.get('_additional', {}).get('vector')
            if vector:
                stats['vector_dimensions'].add(len(vector))
                
        stats['vector_dimensions'] = list(stats['vector_dimensions'])
        return stats