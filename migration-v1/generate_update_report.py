#!/usr/bin/env python3
"""
Generate Update Report Script
Description: Generate comprehensive reports for PostgreSQL datasets update operations
"""

import os
import sys
import json
import logging
import argparse
from datetime import datetime
from dotenv import load_dotenv

# Add src directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from weaviate_to_zilliz_migrator import WeaviateToZillizMigrator
from update_report_generator import UpdateReportGenerator

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def load_stats_from_log(log_file: str) -> dict:
    """Load update statistics from log file (if available)"""
    stats = {
        'total_weaviate_classes': 0,
        'processed_datasets': 0,
        'updated_datasets': 0,
        'failed_updates': 0,
        'not_found_datasets': 0,
        'class_prefix_matches': 0,
        'class_prefix_mismatches': 0,
        'errors': [],
        'mismatch_details': []
    }
    
    if not os.path.exists(log_file):
        logger.warning(f"Log file not found: {log_file}")
        return stats
    
    try:
        with open(log_file, 'r', encoding='utf-8') as f:
            content = f.read()
            
        # Parse statistics from log content
        lines = content.split('\n')
        for line in lines:
            if 'Total Weaviate classes:' in line:
                stats['total_weaviate_classes'] = int(line.split(':')[-1].strip())
            elif 'Processed datasets:' in line:
                stats['processed_datasets'] = int(line.split(':')[-1].strip())
            elif 'Successfully updated:' in line:
                stats['updated_datasets'] = int(line.split(':')[-1].strip())
            elif 'Failed updates:' in line:
                stats['failed_updates'] = int(line.split(':')[-1].strip())
            elif 'Not found datasets:' in line:
                stats['not_found_datasets'] = int(line.split(':')[-1].strip())
            elif 'Class prefix matches:' in line:
                stats['class_prefix_matches'] = int(line.split(':')[-1].strip())
            elif 'Class prefix mismatches:' in line:
                stats['class_prefix_mismatches'] = int(line.split(':')[-1].strip())
                
    except Exception as e:
        logger.error(f"Failed to parse log file: {str(e)}")
        
    return stats


def run_fresh_update_analysis() -> dict:
    """Run a fresh analysis of the current state"""
    logger.info("Running fresh update analysis...")
    
    try:
        # Create migrator instance
        migrator = WeaviateToZillizMigrator()
        
        # Connect to both systems
        logger.info("Connecting to Weaviate...")
        migrator.connect_weaviate()
        
        logger.info("Connecting to PostgreSQL...")
        migrator.connect_postgresql()
        
        # Run analysis (without actually updating)
        logger.info("Analyzing current state...")
        stats = migrator.update_pg_datasets_vector_store_type()
        
        return stats
        
    except Exception as e:
        logger.error(f"Failed to run fresh analysis: {str(e)}")
        return {}


def main():
    """Main function to generate update reports"""
    parser = argparse.ArgumentParser(description="Generate PostgreSQL datasets update reports")
    parser.add_argument('--log-file', '-l', type=str, 
                       help='Path to log file to parse statistics from')
    parser.add_argument('--fresh-analysis', '-f', action='store_true',
                       help='Run fresh analysis instead of parsing log file')
    parser.add_argument('--output-dir', '-o', type=str, default='../reports',
                       help='Output directory for reports (default: ../reports)')
    parser.add_argument('--format', '-t', choices=['all', 'html', 'json', 'txt'], 
                       default='all', help='Report format to generate (default: all)')
    
    args = parser.parse_args()
    
    # Check environment variables
    required_vars = ['PG_HOST', 'PG_DATABASE', 'PG_USER', 'PG_PASSWORD', 'WEAVIATE_ENDPOINT']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars and args.fresh_analysis:
        logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
        logger.error("Please set these variables in your .env file or environment")
        sys.exit(1)
    
    try:
        # Create report generator
        report_generator = UpdateReportGenerator(reports_dir=args.output_dir)
        
        # Set configuration
        config = {
            'weaviate_endpoint': os.getenv('WEAVIATE_ENDPOINT', 'N/A'),
            'postgresql_host': os.getenv('PG_HOST', 'N/A'),
            'postgresql_database': os.getenv('PG_DATABASE', 'N/A')
        }
        
        # Get statistics
        if args.fresh_analysis:
            logger.info("Running fresh analysis...")
            start_time = datetime.now()
            report_generator.set_update_start(start_time, config)
            
            stats = run_fresh_update_analysis()
            
            end_time = datetime.now()
            report_generator.set_update_end(end_time)
            
        elif args.log_file:
            logger.info(f"Parsing statistics from log file: {args.log_file}")
            # Set approximate times for log-based reports
            report_generator.set_update_start(datetime.now(), config)
            report_generator.set_update_end(datetime.now())
            
            stats = load_stats_from_log(args.log_file)
        else:
            logger.error("Please specify either --log-file or --fresh-analysis")
            sys.exit(1)
        
        if not stats:
            logger.error("No statistics available to generate report")
            sys.exit(1)
        
        # Set statistics
        report_generator.set_update_statistics(stats)
        
        # Generate reports based on format choice
        logger.info(f"Generating {args.format} report(s)...")
        
        reports = {}
        if args.format == 'all':
            reports = report_generator.generate_all_reports()
        elif args.format == 'html':
            html_file = report_generator.generate_html_report()
            if html_file:
                reports['html'] = html_file
        elif args.format == 'json':
            json_file = report_generator.generate_json_report()
            if json_file:
                reports['json'] = json_file
        elif args.format == 'txt':
            txt_file = report_generator.generate_summary_report()
            if txt_file:
                reports['txt'] = txt_file
        
        # Print results
        if reports:
            logger.info("\n" + "="*60)
            logger.info("UPDATE REPORTS GENERATED SUCCESSFULLY")
            logger.info("="*60)
            for report_type, filepath in reports.items():
                logger.info(f"{report_type.upper()} Report: {filepath}")
            logger.info("="*60)
            
            # Print summary
            logger.info(f"\nSummary:")
            logger.info(f"- Total Weaviate classes: {stats.get('total_weaviate_classes', 0)}")
            logger.info(f"- Successfully updated: {stats.get('updated_datasets', 0)}")
            logger.info(f"- Failed updates: {stats.get('failed_updates', 0)}")
            logger.info(f"- Not found datasets: {stats.get('not_found_datasets', 0)}")
            
            success_rate = 0
            if stats.get('processed_datasets', 0) > 0:
                success_rate = (stats.get('updated_datasets', 0) / stats.get('processed_datasets', 0)) * 100
            logger.info(f"- Success rate: {success_rate:.1f}%")
        else:
            logger.error("Failed to generate any reports")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.info("Report generation interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Report generation failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()