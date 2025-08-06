# Load Collections 功能说明

## 概述

此功能用于列举 Zilliz Cloud 中的所有 collections 并对每个 collection 执行 load 操作。

## 新增功能

### 1. 获取所有 Collections
- `get_zilliz_collections()`: 使用 REST API 列举 Zilliz Cloud 中的所有 collections
- 使用 API 端点: `POST /v2/vectordb/collections/list`

### 2. 加载所有 Collections  
- `load_all_collections()`: 遍历所有 collections 并调用 load 方法
- 提供详细的成功/失败统计信息

### 3. 独立脚本
- `load_collections.py`: 专门用于加载所有 collections 的独立脚本

## 使用方法

### 方法 1: 使用独立脚本
```bash
cd migration-v1
python load_collections.py
```

### 方法 2: 使用主脚本参数
```bash
cd migration-v1/src
python weaviate_to_zilliz_migrator.py load_collections
```

### 方法 3: 在代码中直接调用
```python
from weaviate_to_zilliz_migrator import WeaviateToZillizMigrator

migrator = WeaviateToZillizMigrator()
migrator.connect_zilliz()
migrator.load_all_collections()
```

## 环境变量配置

确保设置以下环境变量：
- `ZILLIZ_CLOUD_URI`: Zilliz Cloud 集群端点
- `ZILLIZ_CLOUD_API_KEY`: Zilliz Cloud API Token
- `ZILLIZ_CLOUD_DATABASE`: 数据库名称 (默认: "default")

## API 详情

### 列举 Collections API
```bash
curl --request POST \
--url "${CLUSTER_ENDPOINT}/v2/vectordb/collections/list" \
--header "Authorization: Bearer ${TOKEN}" \
--header "Content-Type: application/json" \
-d '{
    "dbName": "_default"
}'
```

### 加载 Collection API
```bash
curl --request POST \
--url "${CLUSTER_ENDPOINT}/v2/vectordb/collections/load" \
--header "Authorization: Bearer ${TOKEN}" \
--header "Content-Type: application/json" \
-d '{
    "collectionName": "collection_name"
}'
```

## 日志记录

- 所有操作都会记录到 `logs/` 目录下
- 日志文件格式: `load_collections_YYYYMMDD_HHMMSS.log`
- 包含详细的成功/失败统计信息

## 错误处理

- 单个 collection 加载失败不会中断整个过程
- 提供详细的错误信息和统计报告
- 支持重试机制 