# 快速入门指南

这是一个快速开始使用 Weaviate 到 Zilliz Cloud 迁移工具的指南。

## 1. 环境准备

### 安装依赖

使用 PDM (推荐):
```bash
pdm install
pdm shell
```

或使用 pip:
```bash
pip install -r requirements.txt
```

### 运行设置脚本
```bash
python scripts/setup.py
```

## 2. 配置

编辑 `.env` 文件，填入你的配置：

```env
# Weaviate 配置
WEAVIATE_ENDPOINT=http://localhost:8080
WEAVIATE_API_KEY=your_weaviate_api_key

# Zilliz Cloud 配置  
ZILLIZ_CLOUD_URI=https://your-cluster.zillizcloud.com
ZILLIZ_CLOUD_API_KEY=your_zilliz_api_key
ZILLIZ_CLOUD_DATABASE=default

# 迁移配置
MIGRATION_BATCH_SIZE=100
```

## 3. 测试连接

```bash
python test_connections.py
```

确保所有测试都通过。

## 4. 预览迁移

在执行实际迁移前，先预览迁移计划：

```bash
python migrate.py --dry-run
```

## 5. 执行迁移

### 迁移所有集合
```bash
python migrate.py
```

### 迁移指定集合
```bash
python migrate.py -c Collection1 Collection2
```

### 启用调试日志
```bash
python migrate.py --log-level DEBUG
```

## 6. 检查结果

迁移完成后，检查：

1. **日志文件**: `logs/migration_*.log`
2. **迁移报告**: `reports/migration_report_*.json`
3. **Zilliz Cloud 控制台**: 验证数据已成功迁移

## 常见问题

### Q: 连接失败怎么办？
A: 
1. 检查网络连接
2. 验证 API 密钥和端点地址
3. 确认服务状态

### Q: 内存不足怎么办？
A: 减小批次大小：
```bash
python migrate.py --batch-size 50
```

### Q: 如何迁移大型数据集？
A: 
1. 分批迁移集合
2. 使用较小的批次大小
3. 监控系统资源

## 高级用法

### 使用示例脚本
```bash
# 简单迁移
python examples/simple_migration.py

# 高级迁移（带分析）
python examples/advanced_migration.py
```

### 清理临时文件
```bash
python scripts/cleanup.py --all
```

## 需要帮助？

1. 查看完整的 [README.md](README.md)
2. 检查日志文件中的错误信息
3. 运行连接测试脚本确认配置