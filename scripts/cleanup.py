#!/usr/bin/env python3
"""
Cleanup script for migration artifacts
"""

import os
import shutil
from pathlib import Path
import argparse
from datetime import datetime, timedelta

def cleanup_logs(days_old: int = 7):
    """Clean up old log files"""
    logs_dir = Path('logs')
    if not logs_dir.exists():
        print("No logs directory found")
        return
    
    cutoff_date = datetime.now() - timedelta(days=days_old)
    removed_count = 0
    
    for log_file in logs_dir.glob('*.log'):
        if log_file.stat().st_mtime < cutoff_date.timestamp():
            log_file.unlink()
            removed_count += 1
            print(f"Removed old log file: {log_file}")
    
    print(f"Cleaned up {removed_count} log files older than {days_old} days")

def cleanup_reports(days_old: int = 30):
    """Clean up old report files"""
    reports_dir = Path('reports')
    if not reports_dir.exists():
        print("No reports directory found")
        return
    
    cutoff_date = datetime.now() - timedelta(days=days_old)
    removed_count = 0
    
    for report_file in reports_dir.glob('*.json'):
        if report_file.stat().st_mtime < cutoff_date.timestamp():
            report_file.unlink()
            removed_count += 1
            print(f"Removed old report file: {report_file}")
    
    print(f"Cleaned up {removed_count} report files older than {days_old} days")

def cleanup_cache():
    """Clean up Python cache files"""
    removed_count = 0
    
    # Remove __pycache__ directories
    for pycache_dir in Path('.').rglob('__pycache__'):
        shutil.rmtree(pycache_dir)
        removed_count += 1
        print(f"Removed cache directory: {pycache_dir}")
    
    # Remove .pyc files
    for pyc_file in Path('.').rglob('*.pyc'):
        pyc_file.unlink()
        removed_count += 1
        print(f"Removed cache file: {pyc_file}")
    
    print(f"Cleaned up {removed_count} cache items")

def main():
    """Main cleanup function"""
    parser = argparse.ArgumentParser(description='Clean up migration artifacts')
    parser.add_argument('--logs-days', type=int, default=7, 
                       help='Remove log files older than N days (default: 7)')
    parser.add_argument('--reports-days', type=int, default=30,
                       help='Remove report files older than N days (default: 30)')
    parser.add_argument('--cache', action='store_true',
                       help='Clean up Python cache files')
    parser.add_argument('--all', action='store_true',
                       help='Clean up everything')
    
    args = parser.parse_args()
    
    print("Migration Cleanup Tool")
    print("="*30)
    
    if args.all or not any([args.cache]):
        # Default cleanup
        cleanup_logs(args.logs_days)
        cleanup_reports(args.reports_days)
        cleanup_cache()
    else:
        if args.cache:
            cleanup_cache()
    
    print("\n✅ Cleanup completed!")

if __name__ == "__main__":
    main()