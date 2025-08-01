# Migration v1 - Direct Migration

Direct migration from Weaviate to Zilliz Cloud with real-time processing.

## Features

- ✅ Direct data transfer
- ✅ Real-time transformation
- ✅ Batch processing
- ✅ Progress tracking
- ✅ Error handling and retry

## Quick Start

1. **Setup Environment**
   ```bash
   cp ../config/env.example .env
   # Edit .env with your configuration
   ```

2. **Test Connections**
   ```bash
   python test_connections.py
   ```

3. **Run Migration**
   ```bash
   # Migrate all collections
   python migrate.py
   
   # Migrate specific collections
   python migrate.py -c Collection1 Collection2
   
   # Dry run (preview only)
   python migrate.py --dry-run
   ```

## Configuration

Edit `.env` file with your settings:

```bash
# Weaviate Configuration
WEAVIATE_ENDPOINT=http://your-weaviate-ip:8080
WEAVIATE_API_KEY=your_weaviate_token

# Zilliz Cloud Configuration
ZILLIZ_CLOUD_URI=https://your-cluster.zillizcloud.com
ZILLIZ_CLOUD_API_KEY=your_zilliz_api_key
ZILLIZ_CLOUD_DATABASE=default

# Migration Configuration
MIGRATION_BATCH_SIZE=100
```

## Files

- `migrate.py` - Main migration script
- `test_connections.py` - Connection testing utility
- `src/` - Core modules
  - `config.py` - Configuration management
  - `weaviate_to_zilliz_migrator.py` - Main migrator class
  - `data_transformer.py` - Data transformation logic
  - `utils.py` - Utility functions

## Documentation

See `../docs/README_v1.md` for detailed documentation.