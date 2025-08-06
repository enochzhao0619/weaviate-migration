# Weaviate to Zilliz Migration Tool Makefile

.PHONY: help setup install test clean migrate dry-run load-collections

# Default target
help:
	@echo "Weaviate to Zilliz Migration Tool"
	@echo "================================="
	@echo ""
	@echo "Available commands:"
	@echo "  setup     - Initial setup (create directories, .env file)"
	@echo "  install   - Install dependencies using PDM"
	@echo "  install-pip - Install dependencies using pip"
	@echo "  test      - Test connections to Weaviate and Zilliz"
	@echo "  dry-run   - Preview migration without execution"
	@echo "  migrate   - Run full migration"
	@echo "  load-collections - Load all collections in Zilliz Cloud"
	@echo "  clean     - Clean up logs and cache files"
	@echo "  clean-all - Deep clean (logs, reports, cache)"
	@echo ""
	@echo "Examples:"
	@echo "  make setup install test dry-run migrate"
	@echo "  make migrate COLLECTIONS='Collection1 Collection2'"
	@echo "  make migrate BATCH_SIZE=50"

# Setup
setup:
	@echo "Setting up migration tool..."
	python scripts/setup.py

# Install dependencies
install:
	@echo "Installing dependencies with PDM..."
	pdm install

install-pip:
	@echo "Installing dependencies with pip..."
	pip install -r requirements.txt

# Test connections
test:
	@echo "Testing connections..."
	python test_connections.py

# Dry run migration
dry-run:
	@echo "Running migration preview..."
	python migrate.py --dry-run $(if $(COLLECTIONS),-c $(COLLECTIONS))

# Run migration
migrate:
	@echo "Running migration..."
	python migrate.py $(if $(COLLECTIONS),-c $(COLLECTIONS)) $(if $(BATCH_SIZE),--batch-size $(BATCH_SIZE)) $(if $(LOG_LEVEL),--log-level $(LOG_LEVEL))

# Load all collections
load-collections:
	@echo "Loading all collections in Zilliz Cloud..."
	cd migration-v1 && python load_collections.py

# Clean up
clean:
	@echo "Cleaning up..."
	python scripts/cleanup.py

clean-all:
	@echo "Deep cleaning..."
	python scripts/cleanup.py --all

# Development targets
dev-setup: setup install test
	@echo "Development environment ready!"

# Quick migration workflow
quick-migrate: test dry-run migrate
	@echo "Quick migration completed!"

# Examples
example-simple:
	@echo "Running simple migration example..."
	python examples/simple_migration.py

example-advanced:
	@echo "Running advanced migration example..."
	python examples/advanced_migration.py