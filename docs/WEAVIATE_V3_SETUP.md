# Weaviate v3 客户端配置指南

本项目已更新为支持 Weaviate v3 Python 客户端，以便连接到 Weaviate 1.19.1 服务器。

## 配置要求

### 1. 环境变量配置

创建 `.env` 文件并配置以下变量：

```bash
# Weaviate 配置 (适用于 Weaviate 1.19.1)
WEAVIATE_ENDPOINT=http://YOUR_WEAVIATE_IP:8080
WEAVIATE_API_KEY=your_weaviate_authentication_token

# Zilliz Cloud 配置
ZILLIZ_CLOUD_URI=https://your-cluster.zillizcloud.com
ZILLIZ_CLOUD_API_KEY=your_zilliz_cloud_api_key
ZILLIZ_CLOUD_DATABASE=default

# 迁移配置
MIGRATION_BATCH_SIZE=100
```

### 2. IP + Token 连接方式

对于使用 IP 地址和 token 的连接方式：

- `WEAVIATE_ENDPOINT`: 您的 Weaviate 服务器 IP 地址和端口
  - 示例: `http://192.168.1.100:8080`
  - 示例: `https://your-weaviate-server.com:8080`

- `WEAVIATE_API_KEY`: 您的 Weaviate 认证 token
  - 这是您从 Weaviate 服务器获得的认证令牌

### 3. 依赖项更新

项目已更新以下依赖项：

```toml
"weaviate-client>=3.26.0,<4.0.0"  # 支持 Weaviate 1.19.1
```

## 使用方法

### 1. 安装依赖

```bash
pip install -r requirements.txt
# 或者使用 pdm
pdm install
```

### 2. 测试连接

```bash
python test_connections.py
```

### 3. 运行迁移

```bash
python migrate.py
# 或者
python src/weaviate_to_zilliz_migrator.py
```

## 主要变更

### Weaviate 连接方式

**之前 (v4 客户端):**
```python
client = weaviate.connect_to_custom(
    http_host=host,
    http_port=port,
    auth_credentials=auth_config
)
```

**现在 (v3 客户端):**
```python
client = weaviate.Client(
    url=weaviate_url,
    auth_client_secret=auth_config,
    timeout_config=(60, 60)
)
```

### 数据查询方式

**之前 (v4 客户端):**
```python
response = collection.query.fetch_objects(include_vector=True)
```

**现在 (v3 客户端):**
```python
result = client.query.get(collection_name, properties).with_additional(["id", "vector"]).do()
```

## 故障排除

### 连接失败

1. 检查 Weaviate 服务器是否可访问
2. 验证 IP 地址和端口是否正确
3. 确认认证 token 是否有效
4. 检查防火墙设置

### 版本兼容性

- Weaviate 服务器版本: 1.19.1
- Python 客户端版本: 3.26.0+
- 支持的认证方式: API Key (Token)

### 常见错误

1. **"Weaviate is not ready"**
   - 检查服务器状态
   - 验证网络连接

2. **认证失败**
   - 检查 API Key 是否正确
   - 确认 token 未过期

3. **超时错误**
   - 增加 timeout_config 值
   - 检查网络延迟