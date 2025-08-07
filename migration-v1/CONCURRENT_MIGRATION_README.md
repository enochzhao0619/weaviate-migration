# 并发迁移功能说明

## 概述

重构后的迁移工具支持多线程并发处理，可以显著提高迁移速度。新的并发功能包括：

1. **集合级并发**：多个集合可以同时进行迁移
2. **批次级并发**：单个集合内的数据批次可以并发处理
3. **线程安全**：所有操作都是线程安全的，包括日志记录和统计更新

## 新增配置参数

在 `.env` 文件中添加以下配置项：

```bash
# 并发配置
MAX_COLLECTION_WORKERS=3    # 同时处理的集合数量（默认: 3）
MAX_BATCH_WORKERS=5         # 每个集合内并发处理的批次数量（默认: 5）

# 现有配置
MIGRATION_BATCH_SIZE=300    # 每个批次的文档数量
MIGRATION_MAX_RETRIES=3     # 重试次数
MIGRATION_RETRY_DELAY=1.0   # 重试延迟（秒）
```

## 使用方法

### 1. 并发模式（默认）
```bash
python migrate.py
```

### 2. 顺序模式
```bash
python migrate.py sequential
```

### 3. 仅加载集合
```bash
python migrate.py load_collections
```

## 性能优化建议

### 线程数配置
- **MAX_COLLECTION_WORKERS**: 建议设置为 2-4，过多会导致数据库连接压力过大
- **MAX_BATCH_WORKERS**: 建议设置为 3-8，根据网络带宽和服务器性能调整

### 批次大小配置
- **MIGRATION_BATCH_SIZE**: 建议范围 100-500
  - 较小的批次：更好的并发性，但开销更大
  - 较大的批次：更高的吞吐量，但并发性较差

## 监控和日志

### 线程识别
- 日志中会显示线程信息：`[Thread-collection-1]` 或 `[Thread-collection-batch-0]`
- 主线程的日志不会显示线程标识

### 新增统计信息
- `completed_batches`: 成功完成的批次数量
- `failed_batches`: 失败的批次数量
- `active_threads`: 当前活跃的线程数量

### 进度显示
- 集合级进度条：显示总体迁移进度
- 批次级进度条：显示每个集合内的批次处理进度

## 故障排除

### 常见问题

1. **连接超时**
   - 减少 `MAX_COLLECTION_WORKERS` 和 `MAX_BATCH_WORKERS`
   - 检查网络连接稳定性

2. **内存使用过高**
   - 减少 `MIGRATION_BATCH_SIZE`
   - 减少并发线程数量

3. **数据不一致**
   - 检查目标数据库的并发写入限制
   - 使用顺序模式进行对比测试

### 性能调优

根据系统资源调整配置：

**小型系统** (CPU < 4核, RAM < 8GB):
```bash
MAX_COLLECTION_WORKERS=2
MAX_BATCH_WORKERS=3
MIGRATION_BATCH_SIZE=200
```

**中型系统** (CPU 4-8核, RAM 8-16GB):
```bash
MAX_COLLECTION_WORKERS=3
MAX_BATCH_WORKERS=5
MIGRATION_BATCH_SIZE=300
```

**大型系统** (CPU > 8核, RAM > 16GB):
```bash
MAX_COLLECTION_WORKERS=4
MAX_BATCH_WORKERS=8
MIGRATION_BATCH_SIZE=500
```

## 技术实现细节

### 线程池架构
- 使用 `ThreadPoolExecutor` 管理线程池
- 集合级线程池：处理多个集合的并发迁移
- 批次级线程池：处理单个集合内的批次并发

### 线程安全机制
- `threading.Lock` 保护共享资源
- 线程本地存储用于数据库连接
- 原子操作更新统计信息

### 错误处理
- 单个批次失败不会影响整个集合迁移
- 单个集合失败不会影响其他集合迁移
- 详细的错误日志和统计信息

## 兼容性

- 向后兼容原有的顺序迁移模式
- 所有现有的配置参数仍然有效
- API接口保持不变