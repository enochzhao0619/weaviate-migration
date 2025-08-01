# GCS 迁移快速开始指南

## 快速设置（5分钟）

### 1. 环境准备

```bash
# 安装依赖
pip install pymilvus weaviate-client google-cloud-storage pandas numpy

# 创建配置文件
cp config/env.gcs.example .env
```

### 2. 配置 GCS

1. **创建 GCS 存储桶**
   ```bash
   gsutil mb gs://your-migration-bucket
   ```

2. **创建服务账户**
   ```bash
   gcloud iam service-accounts create weaviate-migration \
     --display-name="Weaviate Migration Service Account"
   ```

3. **下载密钥文件**
   ```bash
   gcloud iam service-accounts keys create ~/gcs-key.json \
     --iam-account=weaviate-migration@your-project.iam.gserviceaccount.com
   ```

4. **设置权限**
   ```bash
   gsutil iam ch serviceAccount:weaviate-migration@your-project.iam.gserviceaccount.com:objectAdmin gs://your-migration-bucket
   ```

### 3. 配置环境变量

编辑 `.env` 文件：

```bash
# Weaviate 配置
WEAVIATE_ENDPOINT=http://10.15.9.78:8080
WEAVIATE_API_KEY=

# Zilliz Cloud 配置  
ZILLIZ_CLOUD_URI=https://in01-291662cabed17b7.aws-us-west-2.vectordb-sit.zillizcloud.com:19531
ZILLIZ_CLOUD_API_KEY=db_admin:your_password

# GCS 配置
GCS_BUCKET_NAME=your-migration-bucket
GCS_KEY_PATH=/home/user/gcs-key.json
```

### 4. 运行迁移

```bash
# 测试连接
python -c "from migrate_v2_gcs import GCSMigrationConfig; GCSMigrationConfig().validate(); print('✅ 配置验证成功')"

# 开始迁移
python migrate_v2_gcs.py

# 或迁移指定集合
python migrate_v2_gcs.py -c MyCollection AnotherCollection
```

## 验证结果

1. **检查 GCS 文件**
   ```bash
   gsutil ls -r gs://your-migration-bucket/bulk_data_gcs/
   ```

2. **检查 Zilliz Cloud**
   - 登录 Zilliz Cloud 控制台
   - 查看集合和数据导入状态

3. **查看日志**
   ```bash
   tail -f logs/gcs_migration_*.log
   ```

## 故障排除

### 常见错误及解决方案

**错误**: `GCS key file not found`
```bash
# 检查文件路径
ls -la /path/to/your/gcs-key.json
# 更新 .env 文件中的路径
```

**错误**: `Failed to connect to Zilliz Cloud`
```bash
# 检查网络连接
curl -I https://your-cluster.zillizcloud.com
# 验证 API 密钥格式
```

**错误**: `Collection already exists`
```bash
# 删除现有集合或使用不同名称
python -c "from pymilvus import connections, utility; connections.connect('default', uri='your-uri', token='your-token'); utility.drop_collection('collection_name')"
```

## 完成！

迁移完成后，您的数据已从 Weaviate 成功迁移到 Zilliz Cloud。

查看详细文档：`GCS_MIGRATION_README.md`