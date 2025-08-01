"""
Utility functions for Weaviate to Zilliz migration
"""

import json
import logging
import time
from typing import Any, Dict, List, Optional
from functools import wraps
import numpy as np

logger = logging.getLogger(__name__)


def retry_on_failure(max_retries: int = 3, delay: float = 1.0, backoff: float = 2.0):
    """Decorator for retrying failed operations"""
    def decorator(func):
        @wraps(func)
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


def validate_vector(vector: List[float]) -> bool:
    """Validate vector data"""
    if not vector:
        return False
        
    if not isinstance(vector, list):
        return False
        
    if not all(isinstance(x, (int, float)) for x in vector):
        return False
        
    # Check for NaN or infinite values
    if any(np.isnan(x) or np.isinf(x) for x in vector):
        return False
        
    return True


def normalize_vector(vector: List[float]) -> List[float]:
    """Normalize vector to unit length"""
    if not validate_vector(vector):
        raise ValueError("Invalid vector data")
        
    vector_array = np.array(vector)
    norm = np.linalg.norm(vector_array)
    
    if norm == 0:
        return vector
        
    return (vector_array / norm).tolist()


def sanitize_field_name(field_name: str) -> str:
    """Sanitize field name for Zilliz compatibility"""
    # Replace invalid characters
    sanitized = field_name.replace('-', '_').replace(' ', '_').replace('.', '_')
    
    # Ensure it starts with a letter or underscore
    if sanitized and not (sanitized[0].isalpha() or sanitized[0] == '_'):
        sanitized = f"field_{sanitized}"
        
    # Limit length
    if len(sanitized) > 64:
        sanitized = sanitized[:64]
        
    return sanitized or "unknown_field"


def truncate_text(text: str, max_length: int = 65534) -> str:
    """Truncate text to fit within field limits"""
    if not isinstance(text, str):
        text = str(text)
        
    if len(text) <= max_length:
        return text
        
    return text[:max_length-3] + "..."


def extract_text_content(properties: Dict[str, Any], text_fields: List[str] = None) -> str:
    """Extract text content from properties"""
    if text_fields is None:
        text_fields = ['content', 'text', 'title', 'description', 'body', 'summary']
        
    # Try predefined text fields first
    for field in text_fields:
        if field in properties and properties[field]:
            content = str(properties[field]).strip()
            if content:
                return content
                
    # Try to find any string field with substantial content
    for key, value in properties.items():
        if isinstance(value, str) and len(value.strip()) > 10:
            return value.strip()
            
    # Fallback to JSON representation
    return json.dumps(properties, ensure_ascii=False)


def calculate_migration_progress(current: int, total: int) -> Dict[str, Any]:
    """Calculate migration progress statistics"""
    if total == 0:
        return {
            'percentage': 0.0,
            'completed': current,
            'total': total,
            'remaining': 0
        }
        
    percentage = (current / total) * 100
    remaining = total - current
    
    return {
        'percentage': round(percentage, 2),
        'completed': current,
        'total': total,
        'remaining': remaining
    }


def format_duration(seconds: float) -> str:
    """Format duration in human-readable format"""
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.1f}m"
    else:
        hours = seconds / 3600
        return f"{hours:.1f}h"


def estimate_remaining_time(elapsed: float, completed: int, total: int) -> str:
    """Estimate remaining time based on current progress"""
    if completed == 0 or total == 0:
        return "Unknown"
        
    rate = completed / elapsed
    remaining_items = total - completed
    
    if rate == 0:
        return "Unknown"
        
    remaining_seconds = remaining_items / rate
    return format_duration(remaining_seconds)


def validate_collection_name(name: str) -> bool:
    """Validate collection name for Zilliz compatibility"""
    if not name:
        return False
        
    # Check length
    if len(name) > 255:
        return False
        
    # Check for valid characters (alphanumeric, underscore, hyphen)
    if not all(c.isalnum() or c in ['_', '-'] for c in name):
        return False
        
    # Must start with letter or underscore
    if not (name[0].isalpha() or name[0] == '_'):
        return False
        
    return True


def create_safe_collection_name(original_name: str) -> str:
    """Create a safe collection name for Zilliz"""
    if validate_collection_name(original_name):
        return original_name
        
    # Sanitize the name
    safe_name = ''.join(c if c.isalnum() or c in ['_', '-'] else '_' for c in original_name)
    
    # Ensure it starts with letter or underscore
    if safe_name and not (safe_name[0].isalpha() or safe_name[0] == '_'):
        safe_name = f"collection_{safe_name}"
        
    # Limit length
    if len(safe_name) > 255:
        safe_name = safe_name[:255]
        
    return safe_name or "unknown_collection"


def log_memory_usage():
    """Log current memory usage"""
    try:
        import psutil
        process = psutil.Process()
        memory_info = process.memory_info()
        memory_mb = memory_info.rss / 1024 / 1024
        logger.info(f"Memory usage: {memory_mb:.1f} MB")
    except ImportError:
        logger.debug("psutil not available for memory monitoring")
    except Exception as e:
        logger.debug(f"Failed to get memory usage: {str(e)}")


def chunk_list(lst: List[Any], chunk_size: int):
    """Split list into chunks of specified size"""
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]


def safe_json_serialize(obj: Any) -> str:
    """Safely serialize object to JSON"""
    try:
        return json.dumps(obj, ensure_ascii=False, default=str)
    except Exception as e:
        logger.warning(f"Failed to serialize object: {str(e)}")
        return str(obj)