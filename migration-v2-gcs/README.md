# Migration v2 - GCS-based Migration

Two-stage migration using Google Cloud Storage as intermediate storage.

## Features

- ✅ GCS intermediate storage
- ✅ Bulk import performance
- ✅ Parquet/JSON format support
- ✅ Resume capability
- ✅ Large dataset handling
- ✅ Progress monitoring

## Quick Start

1. **Setup GCS**
   - Create GCS bucket
   - Create service account
   - Download JSON key file

2. **Setup Environment**
   ```bash
   cp env.example .env
   # Edit .env with your configuration
   ```

3. **Run Migration**
   ```bash
   # Migrate all collections
   python migrate_v2_gcs.py
   
   # Migrate specific collections
   python migrate_v2_gcs.py -c Collection1 Collection2
   
   # Use JSON format
   python migrate_v2_gcs.py --file-type JSON
   ```

## Configuration

Edit `.env` file with your settings:

```bash
# Weaviate Configuration
WEAVIATE_ENDPOINT=http://10.15.9.78:8080
WEAVIATE_API_KEY=

# Zilliz Cloud Configuration
ZILLIZ_CLOUD_URI=https://your-cluster.zillizcloud.com:19531
ZILLIZ_CLOUD_API_KEY=your_api_key

# GCS Configuration (Required)
GCS_BUCKET_NAME=your-gcs-bucket-name
GCS_KEY_PATH=/path/to/your/service-account-key.json
GCS_REMOTE_PATH=bulk_data_gcs
```

## Migration Process

1. **Backup to GCS**: Data is extracted from Weaviate and written to GCS in Parquet/JSON format
2. **Import from GCS**: Zilliz Cloud bulk import reads data directly from GCS
3. **Progress Monitoring**: Real-time tracking of both backup and import phases

## Files

- `migrate_v2_gcs.py` - Main GCS migration script (standalone)
- `env.example` - Environment configuration template

## Documentation

See `../docs/GCS_MIGRATION_README.md` for detailed documentation.