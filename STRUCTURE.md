# 项目结构说明

## 📁 目录组织

项目已重新组织为清晰的目录结构，不同版本的迁移工具和相关文档分别存放：

```
weaviate-migration/
├── 📄 README.md                    # 主项目说明文档
├── 📄 PROJECT_SUMMARY.md           # 详细项目总结
├── 📄 STRUCTURE.md                 # 本文件 - 项目结构说明
│
├── 📁 migration-v1/                # 版本1：直接迁移
│   ├── 📄 README.md                # v1 说明文档
│   ├── 🐍 migrate.py               # v1 主迁移脚本
│   ├── 🐍 test_connections.py      # v1 连接测试
│   └── 📁 src/                     # v1 源代码模块
│       ├── config.py               # 配置管理
│       ├── weaviate_to_zilliz_migrator.py  # 主迁移器
│       ├── data_transformer.py     # 数据转换
│       └── utils.py                # 工具函数
│
├── 📁 migration-v2-gcs/            # 版本2：GCS 迁移
│   ├── 📄 README.md                # v2 说明文档
│   ├── 🐍 migrate_v2_gcs.py        # v2 GCS 迁移脚本（独立）
│   └── 📄 env.example              # v2 环境配置模板
│
├── 📁 docs/                        # 所有文档
│   ├── 📄 README_v1.md             # v1 详细文档
│   ├── 📄 QUICKSTART_v1.md         # v1 快速开始
│   ├── 📄 GCS_MIGRATION_README.md  # v2 详细文档
│   ├── 📄 QUICKSTART_GCS.md        # v2 快速开始
│   └── 📄 WEAVIATE_V3_SETUP.md     # Weaviate 设置指南
│
├── 📁 examples-v1/                 # v1 示例代码
│   ├── simple_migration.py         # 简单迁移示例
│   └── advanced_migration.py       # 高级迁移示例
│
├── 📁 examples-v2/                 # v2 示例代码
│   └── gcs_migration_example.py    # GCS 迁移示例
│
├── 📁 config/                      # 配置模板
│   ├── env.example                 # v1 环境配置模板
│   └── env.gcs.example            # v2 GCS 环境配置模板
│
└── 📁 scripts/                     # 工具脚本
    ├── cleanup.py                  # 清理脚本
    ├── create_sample_data.py       # 创建测试数据
    ├── data_visualizer.py          # 数据可视化
    └── main.py                     # 主工具脚本
```

## 🎯 版本选择指南

### Migration v1 - 直接迁移
**路径**: `migration-v1/`

**适用场景**:
- 小到中等规模数据集（< 100万向量）
- 快速迁移需求
- 简单的网络环境
- 不需要数据备份

**特点**:
- ✅ 配置简单
- ✅ 实时处理
- ✅ 资源占用少
- ✅ 快速开始

### Migration v2 - GCS 迁移
**路径**: `migration-v2-gcs/`

**适用场景**:
- 大规模数据集（> 100万向量）
- 生产环境迁移
- 需要数据备份
- 网络不稳定环境

**特点**:
- ✅ 高性能批量导入
- ✅ 可恢复迁移
- ✅ GCS 数据备份
- ✅ 支持多种文件格式

## 📖 使用流程

### 1. 选择版本
根据数据规模和需求选择合适的版本

### 2. 查看文档
- v1: `docs/README_v1.md` 和 `docs/QUICKSTART_v1.md`
- v2: `docs/GCS_MIGRATION_README.md` 和 `docs/QUICKSTART_GCS.md`

### 3. 配置环境
- v1: 复制 `config/env.example` 到 `migration-v1/.env`
- v2: 复制 `migration-v2-gcs/env.example` 到 `migration-v2-gcs/.env`

### 4. 运行迁移
- v1: `cd migration-v1 && python migrate.py`
- v2: `cd migration-v2-gcs && python migrate_v2_gcs.py`

## 🔧 开发说明

### 模块依赖
- **v1**: 使用 `src/` 目录下的模块，相互依赖
- **v2**: 独立脚本，无外部模块依赖

### 日志文件
所有版本的日志都存储在根目录的 `logs/` 文件夹中

### 配置管理
- v1: 通过 `src/config.py` 管理
- v2: 直接在脚本中管理，支持环境变量覆盖

## 🚀 快速命令

```bash
# v1 迁移
cd migration-v1 && python migrate.py

# v2 迁移
cd migration-v2-gcs && python migrate_v2_gcs.py

# 连接测试 (v1)
cd migration-v1 && python test_connections.py

# 配置验证 (v2)
cd migration-v2-gcs && python -c "from migrate_v2_gcs import GCSMigrationConfig; GCSMigrationConfig().validate()"
```

---

这种组织结构使得：
- ✅ 不同版本完全独立
- ✅ 文档集中管理
- ✅ 示例代码分类清晰
- ✅ 配置模板统一存放
- ✅ 易于维护和扩展