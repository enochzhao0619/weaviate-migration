#!/usr/bin/env python3
"""
Setup script for Weaviate to Zilliz migration tool
"""

import os
import sys
from pathlib import Path

def create_directories():
    """Create necessary directories"""
    directories = ['logs', 'reports', 'config', 'examples']
    
    for directory in directories:
        Path(directory).mkdir(exist_ok=True)
        print(f"✓ Created directory: {directory}")

def create_env_file():
    """Create .env file from template if it doesn't exist"""
    env_file = Path('.env')
    template_file = Path('config/env.example')
    
    if not env_file.exists() and template_file.exists():
        # Copy template to .env
        with open(template_file, 'r') as f:
            content = f.read()
        
        with open(env_file, 'w') as f:
            f.write(content)
        
        print("✓ Created .env file from template")
        print("⚠️  Please edit .env file with your actual configuration values")
    elif env_file.exists():
        print("✓ .env file already exists")
    else:
        print("❌ Template file not found, cannot create .env")

def check_dependencies():
    """Check if required dependencies are installed"""
    required_packages = [
        'weaviate-client',
        'pymilvus',
        'python-dotenv',
        'tqdm',
        'numpy'
    ]
    
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package.replace('-', '_'))
            print(f"✓ {package} is installed")
        except ImportError:
            missing_packages.append(package)
            print(f"❌ {package} is missing")
    
    if missing_packages:
        print(f"\nTo install missing packages, run:")
        print(f"pip install {' '.join(missing_packages)}")
        print("or")
        print("pdm install")
        return False
    
    return True

def main():
    """Main setup function"""
    print("Weaviate to Zilliz Migration Tool Setup")
    print("="*50)
    
    # Create directories
    create_directories()
    
    # Create .env file
    create_env_file()
    
    # Check dependencies
    deps_ok = check_dependencies()
    
    print("\n" + "="*50)
    if deps_ok:
        print("✅ Setup completed successfully!")
        print("\nNext steps:")
        print("1. Edit .env file with your configuration")
        print("2. Run: python test_connections.py")
        print("3. Run: python migrate.py --dry-run")
        print("4. Run: python migrate.py")
    else:
        print("❌ Setup incomplete - please install missing dependencies")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())