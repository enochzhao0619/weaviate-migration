#!/usr/bin/env python3

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from unittest.mock import Mock, patch
from weaviate_to_zilliz_migrator import WeaviateToZillizMigrator


def test_verify_migration():
    """Simple test for verify_migration method"""
    
    # Create migrator instance
    migrator = WeaviateToZillizMigrator()
    
    # Mock the dependencies
    migrator.get_collection_data = Mock(return_value=[{'id': 1}, {'id': 2}, {'id': 3}])
    migrator.zilliz_client = Mock()
    migrator.zilliz_client.get_collection_stats = Mock(return_value={'rowCount': 3})
    
    # Test case 1: Successful verification with matching counts
    result = migrator.verify_migration('test_collection')
    assert result == True, "Should return True when counts match"
    
    # Test case 2: Fallback to query when get_collection_stats fails
    migrator.zilliz_client.get_collection_stats.side_effect = Exception("Stats not available")
    migrator.zilliz_client.query = Mock(return_value=[{'id': 1}, {'id': 2}, {'id': 3}])
    
    result = migrator.verify_migration('test_collection')
    assert result == True, "Should return True when fallback query works"
    
    # Test case 3: Handle empty query result
    migrator.zilliz_client.query = Mock(return_value=None)
    
    result = migrator.verify_migration('test_collection')
    assert result == True, "Should handle None query result"
    
    # Test case 4: Handle exception in get_collection_data
    migrator.get_collection_data.side_effect = Exception("Connection error")
    
    result = migrator.verify_migration('test_collection')
    assert result == False, "Should return False when exception occurs"
    
    print("All tests passed!")


if __name__ == "__main__":
    test_verify_migration()