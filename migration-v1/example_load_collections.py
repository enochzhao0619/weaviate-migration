#!/usr/bin/env python3
"""
Example: How to use the load collections functionality
"""

import os
import sys
from dotenv import load_dotenv

# Add src directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))
from weaviate_to_zilliz_migrator import WeaviateToZillizMigrator

# Load environment variables
load_dotenv()


def example_list_collections():
    """Example: List all collections in Zilliz Cloud"""
    print("=== Example: List Collections ===")
    
    migrator = WeaviateToZillizMigrator()
    migrator.connect_zilliz()
    
    collections = migrator.get_zilliz_collections()
    print(f"找到 {len(collections)} 个 collections:")
    for i, collection in enumerate(collections, 1):
        print(f"  {i}. {collection}")


def example_load_specific_collection():
    """Example: Load a specific collection"""
    print("\n=== Example: Load Specific Collection ===")
    
    migrator = WeaviateToZillizMigrator()
    migrator.connect_zilliz()
    
    # Get collections first
    collections = migrator.get_zilliz_collections()
    
    if collections:
        collection_name = collections[0]  # Load first collection
        print(f"加载 collection: {collection_name}")
        migrator.load_collection(collection_name)
    else:
        print("没有找到可用的 collections")


def example_load_all_collections():
    """Example: Load all collections"""
    print("\n=== Example: Load All Collections ===")
    
    migrator = WeaviateToZillizMigrator()
    migrator.connect_zilliz()
    
    migrator.load_all_collections()


def main():
    """Run examples"""
    print("Zilliz Cloud Load Collections 功能示例")
    print("=" * 50)
    
    try:
        # Example 1: List collections
        example_list_collections()
        
        # Example 2: Load specific collection
        example_load_specific_collection()
        
        # Example 3: Load all collections
        example_load_all_collections()
        
        print("\n所有示例执行完成!")
        
    except Exception as e:
        print(f"示例执行失败: {str(e)}")


if __name__ == "__main__":
    main() 