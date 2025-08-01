# Weaviate to Zilliz Cloud Migration Tools

A comprehensive set of tools for migrating vector data from Weaviate to Zilliz Cloud, offering multiple migration strategies to suit different needs.

## 🚀 Migration Options

### Migration v1 - Direct Migration
**Path**: `migration-v1/`

Direct migration from Weaviate to Zilliz Cloud using batch processing.

- ✅ Simple and straightforward
- ✅ Real-time data transformation
- ✅ Suitable for smaller datasets
- ✅ Lower storage requirements

**Best for**: Small to medium datasets, direct migrations, testing

### Migration v2 - GCS-based Migration
**Path**: `migration-v2-gcs/`

Two-stage migration using Google Cloud Storage as intermediate storage.

- ✅ Scalable for large datasets
- ✅ Bulk import performance
- ✅ Resume capability
- ✅ Data backup in GCS
- ✅ Supports Parquet and JSON formats

**Best for**: Large datasets, production environments, when backup is needed

## 📁 Project Structure

```
weaviate-migration/
├── migration-v1/              # Direct migration (v1)
│   ├── migrate.py             # Main migration script
│   ├── src/                   # Source modules
│   │   ├── config.py          # Configuration management
│   │   ├── weaviate_to_zilliz_migrator.py
│   │   ├── data_transformer.py
│   │   └── utils.py
│   └── test_connections.py    # Connection testing
│
├── migration-v2-gcs/          # GCS-based migration (v2)
│   ├── migrate_v2_gcs.py      # GCS migration script
│   └── env.example            # Environment configuration
│
├── examples-v1/               # v1 Examples
│   ├── simple_migration.py
│   └── advanced_migration.py
│
├── examples-v2/               # v2 Examples
│   └── gcs_migration_example.py
│
├── docs/                      # Documentation
│   ├── README_v1.md           # v1 Documentation
│   ├── QUICKSTART_v1.md       # v1 Quick start
│   ├── GCS_MIGRATION_README.md # v2 Documentation
│   ├── QUICKSTART_GCS.md      # v2 Quick start
│   └── WEAVIATE_V3_SETUP.md   # Weaviate setup guide
│
├── config/                    # Configuration templates
│   └── env.example            # v1 environment template
│
├── scripts/                   # Utility scripts
├── logs/                      # Migration logs
└── PROJECT_SUMMARY.md         # Detailed project summary
```

## 🚀 Quick Start

### Choose Your Migration Strategy

#### Option 1: Direct Migration (v1)
```bash
cd migration-v1
cp ../config/env.example .env
# Edit .env with your configuration
python migrate.py
```

#### Option 2: GCS Migration (v2)
```bash
cd migration-v2-gcs
cp env.example .env
# Edit .env with your GCS and Zilliz configuration
python migrate_v2_gcs.py
```

## 📊 Comparison

| Feature | v1 Direct | v2 GCS |
|---------|-----------|---------|
| **Setup Complexity** | Simple | Moderate |
| **Storage Required** | Minimal | GCS bucket |
| **Performance** | Good | Excellent |
| **Scalability** | Medium | High |
| **Resume Capability** | Limited | Yes |
| **Data Backup** | No | Yes (in GCS) |
| **Best Dataset Size** | < 1M vectors | > 1M vectors |

## 🔧 Prerequisites

### Common Requirements
- Python 3.8+
- Weaviate instance (accessible)
- Zilliz Cloud account and cluster

### Additional for v2 (GCS)
- Google Cloud Storage bucket
- GCS service account with storage permissions

## 📖 Documentation

- **v1 Migration**: See `docs/README_v1.md` and `docs/QUICKSTART_v1.md`
- **v2 GCS Migration**: See `docs/GCS_MIGRATION_README.md` and `docs/QUICKSTART_GCS.md`
- **Weaviate Setup**: See `docs/WEAVIATE_V3_SETUP.md`

## 🛠 Development

### Project Dependencies
```bash
pip install -r requirements.txt
```

### Running Tests
```bash
# Test v1 connections
cd migration-v1
python test_connections.py

# Test v2 configuration
cd migration-v2-gcs
python -c "from migrate_v2_gcs import GCSMigrationConfig; GCSMigrationConfig().validate()"
```

## 📝 Migration Logs

Both versions create detailed logs in the `logs/` directory:
- Migration progress
- Error details
- Performance metrics
- Final reports

## 🤝 Contributing

1. Choose the appropriate version directory
2. Follow the existing code structure
3. Add tests for new features
4. Update documentation

## 📄 License

This project is provided as-is for migration purposes.

## 🆘 Support

For issues:
1. Check the appropriate documentation in `docs/`
2. Review logs in `logs/` directory
3. Try the connection test scripts
4. Check configuration files

---

**Recommendation**: 
- Use **v1** for quick migrations and smaller datasets
- Use **v2** for production environments and large-scale migrations