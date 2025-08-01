# GCS-based Migration Tool (v2)

基于 Google Cloud Storage 的 Weaviate 到 Zilliz Cloud 迁移工具。该工具首先将数据备份到 GCS，然后通过 Zilliz Cloud 的批量导入功能从 GCS 导入数据。

## 功能特性

- ✅ 使用 GCS 作为中间存储
- ✅ 支持 Parquet 和 JSON 文件格式
- ✅ 批量导入到 Zilliz Cloud
- ✅ 实时进度监控
- ✅ 详细的迁移报告
- ✅ 错误处理和重试机制

## 前置条件

1. **Google Cloud Storage 设置**
   - 创建 GCS 存储桶
   - 创建服务账户并下载 JSON 密钥文件
   - 确保服务账户有存储桶的读写权限

2. **Python 依赖**
   ```bash
   pip install pymilvus weaviate-client google-cloud-storage pandas numpy tqdm
   ```

3. **环境配置**
   - 复制 `config/env.gcs.example` 到 `config/.env`
   - 填写必要的配置信息

## 配置说明

### 环境变量

创建 `.env` 文件并配置以下变量：

```bash
# Weaviate 配置
WEAVIATE_ENDPOINT=http://10.15.9.78:8080
WEAVIATE_API_KEY=

# Zilliz Cloud 配置
ZILLIZ_CLOUD_URI=https://your-cluster.zillizcloud.com:19531
ZILLIZ_CLOUD_API_KEY=your_api_key
ZILLIZ_CLOUD_DATABASE=default

# GCS 配置（必需）
GCS_BUCKET_NAME=your-gcs-bucket-name
GCS_KEY_PATH=/path/to/your/service-account-key.json
GCS_REMOTE_PATH=bulk_data_gcs

# 可选配置
GCS_SEGMENT_SIZE=536870912  # 512MB
MIGRATION_BATCH_SIZE=100
```

### GCS 服务账户权限

确保您的 GCS 服务账户具有以下权限：
- `storage.objects.create`
- `storage.objects.delete`
- `storage.objects.get`
- `storage.objects.list`

## 使用方法

### 1. 命令行使用

```bash
# 迁移所有集合
python migrate_v2_gcs.py

# 迁移指定集合
python migrate_v2_gcs.py -c Collection1 Collection2

# 使用不同的文件格式
python migrate_v2_gcs.py --file-type JSON

# 启用调试日志
python migrate_v2_gcs.py --log-level DEBUG

# 覆盖 GCS 配置
python migrate_v2_gcs.py --gcs-bucket my-bucket --gcs-key-path /path/to/key.json
```

### 2. 编程使用

```python
from migrate_v2_gcs import WeaviateToZillizGCSMigrator, GCSMigrationConfig

# 创建配置
config = GCSMigrationConfig()
config.gcs_bucket_name = "your-gcs-bucket"
config.gcs_key_path = "/path/to/service-account-key.json"

# 创建迁移器并运行
migrator = WeaviateToZillizGCSMigrator(config)
stats = migrator.run_migration()

print(f"Migrated {stats['migrated_documents']} documents")
```

## 迁移流程

1. **连接验证**
   - 连接到 Weaviate 实例
   - 连接到 Zilliz Cloud
   - 验证 GCS 访问权限

2. **数据备份到 GCS**
   - 从 Weaviate 读取集合数据
   - 使用 RemoteBulkWriter 写入 GCS
   - 生成 Parquet/JSON 文件

3. **从 GCS 导入到 Zilliz Cloud**
   - 创建批量导入任务
   - 监控导入进度
   - 验证导入结果

## 监控和日志

### 日志文件
- 主日志：`logs/gcs_migration_YYYYMMDD_HHMMSS.log`
- 迁移报告：`logs/gcs_migration_report_YYYYMMDD_HHMMSS.json`

### 进度监控
工具会实时显示：
- 当前处理的集合
- 数据备份进度
- GCS 文件创建状态
- 导入任务进度

## 故障排除

### 常见问题

1. **GCS 认证失败**
   ```
   Error: Failed to authenticate with GCS
   ```
   - 检查服务账户密钥文件路径
   - 确认服务账户权限

2. **Zilliz Cloud 连接失败**
   ```
   Error: Failed to connect to Zilliz Cloud
   ```
   - 检查 URI 和 API 密钥
   - 确认网络连接

3. **导入任务失败**
   ```
   Error: Import failed: Invalid file format
   ```
   - 检查文件格式设置
   - 验证 schema 匹配

### 调试技巧

1. 启用调试日志：
   ```bash
   python migrate_v2_gcs.py --log-level DEBUG
   ```

2. 检查 GCS 文件：
   - 登录 Google Cloud Console
   - 查看存储桶中的文件
   - 验证文件大小和格式

3. 监控 Zilliz Cloud：
   - 使用 Zilliz Cloud 控制台
   - 检查导入任务状态
   - 查看错误日志

## 性能优化

### 配置调优

1. **段大小调整**
   ```bash
   export GCS_SEGMENT_SIZE=1073741824  # 1GB
   ```

2. **并发控制**
   - 根据网络带宽调整批次大小
   - 考虑 GCS 和 Zilliz Cloud 的限制

3. **文件格式选择**
   - Parquet：更好的压缩和查询性能
   - JSON：更好的可读性和调试

## 安全注意事项

1. **密钥管理**
   - 不要将服务账户密钥提交到版本控制
   - 使用环境变量或密钥管理服务

2. **网络安全**
   - 使用 HTTPS 连接
   - 考虑使用 VPN 或私有网络

3. **数据加密**
   - GCS 自动加密存储的数据
   - 考虑使用客户管理的加密密钥

## 示例

查看 `examples/gcs_migration_example.py` 获取完整的使用示例。

## 支持

如果遇到问题，请：
1. 检查日志文件
2. 启用调试模式
3. 查看故障排除部分
4. 提交 issue 并附上相关日志