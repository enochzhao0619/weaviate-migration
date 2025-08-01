# Weaviate to Zilliz Cloud Migration Tool

一个完整的向量数据库迁移工具，用于将数据从 Weaviate 迁移到 Zilliz Cloud (Milvus)。

## 功能特性

- **完整的数据迁移**: 支持向量、文本内容和元数据的完整迁移
- **批量处理**: 支持大规模数据的批量迁移，可配置批次大小
- **模式自动映射**: 自动分析 Weaviate 模式并创建对应的 Zilliz 集合
- **错误处理**: 完善的错误处理和重试机制
- **进度跟踪**: 实时显示迁移进度和统计信息
- **验证功能**: 迁移后自动验证数据完整性
- **详细日志**: 完整的迁移日志和报告生成
- **预览模式**: 支持预览迁移计划而不执行实际迁移

## 安装

### 使用 PDM (推荐)

```bash
# 安装依赖
pdm install

# 激活虚拟环境
pdm shell
```

### 使用 pip

```bash
# 安装依赖
pip install -r requirements.txt
```

## 配置

1. 复制配置文件模板：
```bash
cp config/env.example .env
```

2. 编辑 `.env` 文件，填入你的配置信息：

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

### 必需的配置项

- `ZILLIZ_CLOUD_URI`: Zilliz Cloud 集群地址
- `ZILLIZ_CLOUD_API_KEY`: Zilliz Cloud API 密钥

### 可选的配置项

- `WEAVIATE_ENDPOINT`: Weaviate 服务地址 (默认: http://localhost:8080)
- `WEAVIATE_API_KEY`: Weaviate API 密钥 (如果需要认证)
- `ZILLIZ_CLOUD_DATABASE`: 目标数据库名称 (默认: default)
- `MIGRATION_BATCH_SIZE`: 批处理大小 (默认: 100)

## 使用方法

### 基本用法

```bash
# 迁移所有集合
python migrate.py

# 迁移指定集合
python migrate.py -c Collection1 Collection2

# 预览迁移计划（不执行实际迁移）
python migrate.py --dry-run

# 启用调试日志
python migrate.py --log-level DEBUG

# 自定义批次大小
python migrate.py --batch-size 50

# 跳过迁移验证
python migrate.py --skip-verification
```

### 高级用法

```bash
# 组合使用多个选项
python migrate.py -c MyCollection --batch-size 200 --log-level INFO --dry-run
```

### 命令行参数

- `-c, --collections`: 指定要迁移的集合名称
- `--dry-run`: 预览模式，不执行实际迁移
- `--log-level`: 设置日志级别 (DEBUG, INFO, WARNING, ERROR)
- `--batch-size`: 覆盖默认批次大小
- `--skip-verification`: 跳过迁移后验证

## 项目结构

```
weaviate-migration/
├── src/                                    # 源代码目录
│   ├── weaviate_to_zilliz_migrator.py     # 主迁移类
│   ├── config.py                          # 配置管理
│   ├── utils.py                           # 工具函数
│   ├── data_transformer.py               # 数据转换器
│   └── __init__.py
├── config/
│   └── env.example                        # 配置文件模板
├── logs/                                  # 日志文件目录
├── reports/                               # 迁移报告目录
├── migrate.py                             # 主入口脚本
├── requirements.txt                       # Python 依赖
├── pyproject.toml                         # 项目配置
└── README.md                              # 项目说明
```

## 迁移流程

1. **连接验证**: 验证与 Weaviate 和 Zilliz Cloud 的连接
2. **模式分析**: 分析 Weaviate 集合的模式结构
3. **集合创建**: 在 Zilliz Cloud 中创建对应的集合
4. **数据转换**: 将 Weaviate 数据格式转换为 Zilliz 格式
5. **批量迁移**: 分批迁移数据到 Zilliz Cloud
6. **验证检查**: 验证迁移数据的完整性
7. **报告生成**: 生成详细的迁移报告

## 数据映射

### 字段映射

| Weaviate | Zilliz Cloud | 说明 |
|----------|-------------|------|
| `_additional.id` | `id` | 文档唯一标识符 |
| `_additional.vector` | `vector` | 向量数据 |
| 文本属性 | `text` | 提取的主要文本内容 |
| 所有属性 | `metadata` | JSON 格式的元数据 |
| 各个属性 | 对应字段 | 根据数据类型映射 |

### 数据类型映射

| Weaviate 类型 | Zilliz 类型 |
|--------------|------------|
| text/string | VARCHAR |
| int/integer | INT64 |
| number/float | DOUBLE |
| boolean | BOOL |

## 日志和报告

### 日志文件
- 位置: `logs/migration_YYYYMMDD_HHMMSS.log`
- 包含详细的迁移过程记录和错误信息

### 迁移报告
- 位置: `reports/migration_report_YYYYMMDD_HHMMSS.json`
- 包含完整的迁移统计信息和配置详情

## 故障排除

### 常见问题

1. **连接失败**
   - 检查网络连接和防火墙设置
   - 验证 API 密钥和端点地址
   - 确认服务状态

2. **内存不足**
   - 减小批次大小 (`MIGRATION_BATCH_SIZE`)
   - 分批迁移大型集合

3. **数据类型错误**
   - 检查 Weaviate 数据格式
   - 查看日志中的详细错误信息

4. **权限问题**
   - 确认 API 密钥有足够权限
   - 检查数据库访问权限

### 调试技巧

1. 使用 `--dry-run` 模式预览迁移计划
2. 设置 `--log-level DEBUG` 获取详细日志
3. 先迁移小的测试集合验证配置
4. 检查生成的迁移报告

## 性能优化

1. **批次大小**: 根据数据大小和网络条件调整批次大小
2. **并发控制**: 工具已内置适当的延迟和重试机制
3. **内存管理**: 大型数据集建议分批迁移
4. **网络优化**: 确保稳定的网络连接

## 安全注意事项

1. **API 密钥**: 妥善保管 API 密钥，不要提交到版本控制
2. **网络安全**: 使用 HTTPS 连接和 VPN（如适用）
3. **数据备份**: 迁移前备份重要数据
4. **访问控制**: 限制迁移工具的网络访问权限

## 许可证

MIT License

## 支持

如果遇到问题或需要帮助，请：

1. 查看日志文件获取详细错误信息
2. 检查配置是否正确
3. 参考故障排除部分
4. 提交 Issue 并附上相关日志