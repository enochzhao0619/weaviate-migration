# Weaviate to Zilliz Cloud Migration Tool (Optimized)

优化版本的 Weaviate 到 Zilliz Cloud 迁移工具，专注于串行批处理以避免内存溢出问题。

## 主要特性

- **串行批处理**: 默认每批处理 250 条文档，避免 OOM 问题
- **Cursor 分页**: 使用 Weaviate cursor 功能逐步获取数据
- **内存优化**: 边读取边处理边上传，不会一次性加载所有数据
- **自动集合创建**: 首批数据处理时自动创建 Zilliz 集合
- **错误恢复**: 单批失败不影响整体迁移进程

## 配置

### 环境变量 (.env 文件)

```bash
# Weaviate Configuration
WEAVIATE_ENDPOINT=http://your-weaviate-endpoint
WEAVIATE_API_KEY=your_weaviate_api_key

# Zilliz Cloud Configuration
ZILLIZ_CLOUD_URI=https://your-zilliz-endpoint
ZILLIZ_CLOUD_API_KEY=your_zilliz_api_key
ZILLIZ_CLOUD_DATABASE=default

# Migration Configuration
MIGRATION_BATCH_SIZE=250
MIGRATION_MAX_RETRIES=3
MIGRATION_RETRY_DELAY=1.0
```

## 使用方法

### 1. 简单启动（推荐）

```bash
# 迁移所有集合
python run_migration.py

# 限制每个集合的文档数量（用于测试）
python run_migration.py --limit 1000

# 迁移指定集合
python run_migration.py --collections Collection1 Collection2

# 查看帮助
python run_migration.py --help
```

### 2. 完整功能启动

```bash
# 基本迁移
python migrate.py

# 预览迁移计划
python migrate.py --dry-run

# 只下载数据不迁移
python migrate.py --mode download

# 自定义批处理大小
python migrate.py --batch-size 500

# 迁移指定集合
python migrate.py -c Collection1 Collection2

# 调试模式
python migrate.py --log-level DEBUG
```

## 迁移流程

### 串行批处理流程

1. **连接检查**: 验证 Weaviate 和 Zilliz 连接
2. **集合检查**: 检查 Zilliz 中是否已存在同名集合
3. **获取 Schema**: 从 Weaviate 获取集合结构信息
4. **批量处理**:
   - 使用 cursor 分页从 Weaviate 获取 250 条数据
   - 首批数据时自动创建 Zilliz 集合
   - 转换数据格式
   - 上传到 Zilliz
   - 重复直到所有数据处理完成
5. **验证**: 比较源和目标的文档数量

### 内存优化策略

- **小批量处理**: 默认 250 条/批，可通过 `MIGRATION_BATCH_SIZE` 调整
- **流式处理**: 不会一次性加载所有数据到内存
- **即时释放**: 每批处理完成后立即释放内存
- **Cursor 分页**: 使用 Weaviate 的 cursor 功能避免重复数据

## 性能调优

### 批处理大小调整

```bash
# 内存较小的环境（推荐 100-250）
export MIGRATION_BATCH_SIZE=100

# 内存充足的环境（可使用 500-1000）
export MIGRATION_BATCH_SIZE=500

# 或通过命令行参数
python migrate.py --batch-size 500
```

### 建议配置

| 内存大小 | 推荐批处理大小 | 说明 |
|---------|---------------|------|
| < 4GB   | 100-150       | 保守配置，避免 OOM |
| 4-8GB   | 200-300       | 默认配置 |
| > 8GB   | 400-500       | 高性能配置 |

## 日志和监控

### 日志输出

- **控制台**: 实时显示迁移进度
- **文件**: `logs/migration.log` 详细日志记录
- **批次信息**: 每批处理的详细状态

### 进度监控

```
[1/3] Starting migration for collection: MyCollection
Processing batch 1 (up to 250 documents)...
Fetched 250 documents in batch 1
Creating Zilliz collection for first batch...
Transforming batch 1 data for Zilliz...
Uploading 250 documents to Zilliz...
✓ Batch 1 completed: 250 documents uploaded
Total migrated so far: 250/1000
```

## 错误处理

### 常见问题解决

1. **内存不足 (OOM)**
   ```bash
   # 减小批处理大小
   export MIGRATION_BATCH_SIZE=100
   ```

2. **网络超时**
   ```bash
   # 增加重试次数和延迟
   export MIGRATION_MAX_RETRIES=5
   export MIGRATION_RETRY_DELAY=2.0
   ```

3. **连接问题**
   - 检查网络连接
   - 验证 API 密钥
   - 确认端点地址

### 错误恢复

- 单批失败不会终止整个迁移
- 自动跳过已存在的集合
- 支持断点续传（重新运行会跳过已迁移的集合）

## 验证和测试

### 迁移验证

```bash
# 预览模式（不执行实际迁移）
python migrate.py --dry-run

# 小批量测试
python run_migration.py --limit 100

# 下载数据进行离线分析
python migrate.py --mode download
```

### 数据一致性检查

迁移完成后会自动验证：
- 文档数量对比
- 基本数据结构检查
- 向量维度验证

## 注意事项

1. **串行处理**: 此版本只支持串行处理，不支持并发
2. **内存优化**: 专为大数据集设计，避免内存溢出
3. **批量上传**: 数据按批次上传，首批时创建集合
4. **幂等操作**: 重复运行会跳过已存在的集合

## 故障排除

### 检查连接

```bash
# 测试基本连接
python -c "
import sys
sys.path.append('src')
from weaviate_to_zilliz_migrator import WeaviateToZillizMigrator
m = WeaviateToZillizMigrator()
m.connect_weaviate()
m.connect_zilliz()
print('连接成功')
"
```

### 查看详细日志

```bash
# 启用调试日志
python migrate.py --log-level DEBUG

# 查看日志文件
tail -f logs/migration.log
```

## 升级说明

从之前版本升级的主要变化：

1. **移除并发支持**: 专注于串行批处理
2. **优化内存使用**: 使用 cursor 分页和小批量处理
3. **简化配置**: 减少不必要的配置选项
4. **改进错误处理**: 更好的错误恢复机制

这个优化版本专门为处理大数据集而设计，确保在有限内存环境下也能稳定运行。
