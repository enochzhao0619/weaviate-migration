# 快速使用指南 - Load Collections

## 功能概述
此功能用于列举 Zilliz Cloud 中的所有 collections 并对每个 collection 执行 load 操作，确保 collections 可以正常查询。

## 前置条件
确保以下环境变量已设置：
- `ZILLIZ_CLOUD_URI`: Zilliz Cloud 集群端点
- `ZILLIZ_CLOUD_API_KEY`: Zilliz Cloud API Token  
- `ZILLIZ_CLOUD_DATABASE`: 数据库名称 (可选，默认: "default")

## 快速开始

### 方法 1: 使用 Makefile (推荐)
```bash
make load-collections
```

### 方法 2: 直接运行脚本
```bash
cd migration-v1
python load_collections.py
```

### 方法 3: 作为参数运行主脚本
```bash
cd migration-v1/src
python weaviate_to_zilliz_migrator.py load_collections
```

## 输出示例
```
2024-01-15 10:30:00 - Loading all collections in Zilliz Cloud
2024-01-15 10:30:01 - Found 3 collections in Zilliz Cloud: ['collection1', 'collection2', 'collection3']
2024-01-15 10:30:01 - Found 3 collections to load
2024-01-15 10:30:01 - Loading collection: collection1
2024-01-15 10:30:02 - Successfully loaded collection collection1
2024-01-15 10:30:02 - Loading collection: collection2
2024-01-15 10:30:03 - Successfully loaded collection collection2
2024-01-15 10:30:03 - Loading collection: collection3
2024-01-15 10:30:04 - Successfully loaded collection collection3

Load Summary:
Total collections: 3
Successfully loaded: 3
Failed to load: 0
Successfully loaded collections:
  ✓ collection1
  ✓ collection2
  ✓ collection3
```

## 日志文件
所有操作都会记录到 `logs/load_collections_YYYYMMDD_HHMMSS.log` 文件中。

## 测试功能
运行测试脚本验证功能：
```bash
cd migration-v1
python test_load_collections.py
```

## 查看示例
运行示例脚本了解用法：
```bash
cd migration-v1
python example_load_collections.py
```

## 故障排除

### 常见错误
1. **连接失败**: 检查 `ZILLIZ_CLOUD_URI` 和 `ZILLIZ_CLOUD_API_KEY` 是否正确
2. **没有找到 collections**: 确认数据库名称配置正确
3. **权限错误**: 确保 API Key 有足够的权限

### 调试步骤
1. 检查环境变量配置
2. 查看日志文件获取详细错误信息
3. 运行测试脚本验证连接

## API 参考
此功能使用以下 Zilliz Cloud REST API:
- `POST /v2/vectordb/collections/list` - 列举 collections
- `POST /v2/vectordb/collections/load` - 加载 collection 