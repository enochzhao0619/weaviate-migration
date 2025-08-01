#!/usr/bin/env python3
"""
GCS Migration Example - 基于 GCS 的迁移示例
本示例展示如何使用 GCS 作为中间存储进行 Weaviate 到 Zilliz Cloud 的迁移
"""

import os
import json
import logging
import sys
from pathlib import Path

# Add parent directory to path to import modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from migrate_v2_gcs import WeaviateToZillizGCSMigrator, GCSMigrationConfig

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    """运行 GCS 迁移示例"""
    
    # 配置示例 - 请根据您的实际情况修改这些值
    config = GCSMigrationConfig()
    
    # 覆盖默认配置（如果需要）
    config.weaviate_endpoint = "http://10.15.9.78:8080"
    config.zilliz_uri = "https://in01-291662cabed17b7.aws-us-west-2.vectordb-sit.zillizcloud.com:19531"
    config.zilliz_token = "db_admin:your_password"
    config.gcs_bucket_name = "your-gcs-bucket-name"
    config.gcs_key_path = "/path/to/your/service-account-key.json"
    
    try:
        # 验证配置
        config.validate()
        logger.info("Configuration validated successfully")
        
        # 创建迁移器
        migrator = WeaviateToZillizGCSMigrator(config)
        
        # 运行迁移 - 可以指定特定的集合或迁移所有集合
        # collections_to_migrate = ["MyCollection", "AnotherCollection"]  # 指定集合
        collections_to_migrate = None  # None 表示迁移所有集合
        
        logger.info("Starting GCS-based migration...")
        stats = migrator.run_migration(collections_to_migrate)
        
        # 打印迁移统计信息
        logger.info("Migration completed!")
        logger.info(f"Successful collections: {len(stats['successful_collections'])}")
        logger.info(f"Failed collections: {len(stats['failed_collections'])}")
        logger.info(f"Total documents migrated: {stats['migrated_documents']}")
        logger.info(f"GCS files created: {len(stats['gcs_files'])}")
        
        # 保存详细报告
        report_file = f"gcs_migration_report_{int(time.time())}.json"
        with open(report_file, 'w') as f:
            # Convert datetime objects for JSON serialization
            stats_copy = stats.copy()
            if stats_copy['start_time']:
                stats_copy['start_time'] = stats_copy['start_time'].isoformat()
            if stats_copy['end_time']:
                stats_copy['end_time'] = stats_copy['end_time'].isoformat()
            json.dump(stats_copy, f, indent=2)
        
        logger.info(f"Detailed report saved to: {report_file}")
        
    except Exception as e:
        logger.error(f"Migration failed: {str(e)}")
        raise

if __name__ == "__main__":
    import time
    main()