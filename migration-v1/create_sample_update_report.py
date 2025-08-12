#!/usr/bin/env python3
"""
Create Sample Update Report
Description: Create a sample update report to demonstrate the reporting functionality
"""

import os
import sys
from datetime import datetime, timedelta

# Add src directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from update_report_generator import UpdateReportGenerator

def create_sample_report():
    """Create a sample update report with mock data"""
    
    # Create report generator
    report_generator = UpdateReportGenerator(reports_dir="../reports")
    
    # Set mock configuration
    config = {
        'weaviate_endpoint': 'http://localhost:8080',
        'postgresql_host': 'localhost',
        'postgresql_database': 'dify',
        'update_type': 'weaviate_to_milvus_vector_store_type'
    }
    
    # Set mock times
    start_time = datetime.now() - timedelta(minutes=15)
    end_time = datetime.now()
    
    report_generator.set_update_start(start_time, config)
    report_generator.set_update_end(end_time)
    
    # Create mock statistics
    mock_stats = {
        'total_weaviate_classes': 25,
        'processed_datasets': 23,
        'updated_datasets': 18,
        'failed_updates': 2,
        'not_found_datasets': 3,
        'class_prefix_matches': 20,
        'class_prefix_mismatches': 3,
        'errors': [
            'Failed to update dataset for class ProductCatalog: Connection timeout',
            'Failed to update dataset for class UserProfiles: Invalid JSON structure'
        ],
        'mismatch_details': [
            {
                'dataset_id': 'dataset_001',
                'class_name': 'DocumentChunk',
                'class_prefix': 'Vector_index_e60b868e_20d2_47bd_bf17_5af69d1a172f_Node'
            },
            {
                'dataset_id': 'dataset_002', 
                'class_name': 'KnowledgeBase',
                'class_prefix': 'Vector_index_f71c979f_31e3_58ce_cf28_6bg80e2b283g_Node'
            },
            {
                'dataset_id': 'dataset_003',
                'class_name': 'ChatHistory',
                'class_prefix': 'Vector_index_a82d090a_42f4_69df_dg39_7ch91f3c394h_Node'
            }
        ]
    }
    
    # Set statistics
    report_generator.set_update_statistics(mock_stats)
    
    # Add some sample successful updates
    successful_classes = [
        'DocumentChunk', 'KnowledgeBase', 'ChatHistory', 'UserProfiles', 'ProductCatalog',
        'SearchIndex', 'ContentMetadata', 'FileStorage', 'ApiLogs', 'SystemConfig',
        'UserSessions', 'DataCache', 'WorkflowSteps', 'ModelConfig', 'EmbeddingCache',
        'QueryResults', 'ConversationFlow', 'DataProcessing'
    ]
    
    for i, class_name in enumerate(successful_classes):
        dataset_id = f"dataset_{str(i+1).zfill(3)}"
        report_generator.add_successful_update(dataset_id, class_name)
    
    # Add some failed updates
    failed_classes = ['ProductCatalog', 'UserProfiles']
    failed_errors = [
        'Connection timeout during update operation',
        'Invalid JSON structure in index_struct field'
    ]
    
    for class_name, error in zip(failed_classes, failed_errors):
        dataset_id = f"dataset_{class_name.lower()}_failed"
        report_generator.add_failed_update(dataset_id, class_name, error)
    
    # Add some not found datasets
    not_found_classes = ['LegacyData', 'TempStorage', 'BackupIndex']
    for class_name in not_found_classes:
        dataset_id = f"dataset_{class_name.lower()}"
        report_generator.add_not_found_dataset(dataset_id, class_name)
    
    # Generate all reports
    print("Generating sample update reports...")
    reports = report_generator.generate_all_reports()
    
    if reports:
        print("\n" + "="*60)
        print("SAMPLE UPDATE REPORTS GENERATED")
        print("="*60)
        for report_type, filepath in reports.items():
            print(f"{report_type.upper()} Report: {filepath}")
        print("="*60)
        
        print(f"\nSample Report Summary:")
        print(f"- Total Weaviate classes: {mock_stats['total_weaviate_classes']}")
        print(f"- Successfully updated: {mock_stats['updated_datasets']}")
        print(f"- Failed updates: {mock_stats['failed_updates']}")
        print(f"- Not found datasets: {mock_stats['not_found_datasets']}")
        print(f"- Class prefix mismatches: {mock_stats['class_prefix_mismatches']}")
        
        success_rate = (mock_stats['updated_datasets'] / mock_stats['processed_datasets']) * 100
        print(f"- Success rate: {success_rate:.1f}%")
        
        print(f"\nYou can now view the reports:")
        if 'html' in reports:
            print(f"- Open {reports['html']} in your browser for the visual report")
        if 'summary' in reports:
            print(f"- View {reports['summary']} for a text summary")
        if 'json' in reports:
            print(f"- Check {reports['json']} for detailed JSON data")
    else:
        print("Failed to generate sample reports")

if __name__ == "__main__":
    create_sample_report()