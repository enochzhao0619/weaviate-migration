#!/usr/bin/env python3
"""
Migration Report Generator
Generates comprehensive reports for Weaviate to Zilliz migration results
"""

import json
import os
from datetime import datetime
from typing import Dict, List, Any, Optional
import logging

logger = logging.getLogger(__name__)


class MigrationReportGenerator:
    """Generates detailed migration reports in multiple formats"""
    
    def __init__(self, reports_dir: str = "reports"):
        self.reports_dir = reports_dir
        self.migration_data = {
            'migration_id': f"migration_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            'start_time': None,
            'end_time': None,
            'total_duration': None,
            'collections': {},  # Collection name -> collection data
            'summary': {
                'total_collections': 0,
                'successful_collections': 0,
                'failed_collections': 0,
                'skipped_collections': 0,
                'total_documents_processed': 0,
                'total_documents_migrated': 0
            },
            'configuration': {},
            'errors': []
        }
        
        # Ensure reports directory exists
        os.makedirs(self.reports_dir, exist_ok=True)
        
    def set_migration_start(self, start_time: datetime, config: Dict[str, Any]):
        """Set migration start time and configuration"""
        self.migration_data['start_time'] = start_time.isoformat()
        self.migration_data['configuration'] = {
            'weaviate_endpoint': config.get('weaviate_endpoint', 'N/A'),
            'zilliz_uri': config.get('zilliz_uri', 'N/A'),
            'batch_size': config.get('batch_size', 'N/A'),
            'max_retries': config.get('max_retries', 'N/A')
        }
        
    def set_migration_end(self, end_time: datetime):
        """Set migration end time and calculate duration"""
        self.migration_data['end_time'] = end_time.isoformat()
        if self.migration_data['start_time']:
            start = datetime.fromisoformat(self.migration_data['start_time'])
            duration = end_time - start
            self.migration_data['total_duration'] = str(duration)
            
    def add_collection_start(self, collection_name: str, schema: Dict[str, Any], 
                           weaviate_doc_count: int):
        """Add collection information when migration starts"""
        self.migration_data['collections'][collection_name] = {
            'name': collection_name,
            'status': 'processing',
            'schema': schema,
            'weaviate_document_count': weaviate_doc_count,
            'zilliz_document_count': 0,
            'migrated_documents': 0,
            'failed_documents': 0,
            'start_time': datetime.now().isoformat(),
            'end_time': None,
            'duration': None,
            'error_message': None,
            'batches_processed': 0,
            'batches_failed': 0
        }
        
    def update_collection_progress(self, collection_name: str, batch_size: int, 
                                 success: bool = True):
        """Update collection progress after each batch"""
        if collection_name in self.migration_data['collections']:
            collection = self.migration_data['collections'][collection_name]
            collection['batches_processed'] += 1
            if success:
                collection['migrated_documents'] += batch_size
            else:
                collection['batches_failed'] += 1
                collection['failed_documents'] += batch_size
                
    def set_collection_result(self, collection_name: str, status: str, 
                            zilliz_doc_count: int = 0, error_message: str = None):
        """Set final result for a collection"""
        if collection_name in self.migration_data['collections']:
            collection = self.migration_data['collections'][collection_name]
            collection['status'] = status  # 'success', 'failed', 'skipped'
            collection['zilliz_document_count'] = zilliz_doc_count
            collection['end_time'] = datetime.now().isoformat()
            collection['error_message'] = error_message
            
            # Calculate duration
            if collection['start_time']:
                start = datetime.fromisoformat(collection['start_time'])
                end = datetime.fromisoformat(collection['end_time'])
                collection['duration'] = str(end - start)
                
            # Update summary
            self.migration_data['summary']['total_collections'] += 1
            if status == 'success':
                self.migration_data['summary']['successful_collections'] += 1
                self.migration_data['summary']['total_documents_migrated'] += collection['migrated_documents']
            elif status == 'failed':
                self.migration_data['summary']['failed_collections'] += 1
            elif status == 'skipped':
                self.migration_data['summary']['skipped_collections'] += 1
                
            self.migration_data['summary']['total_documents_processed'] += collection['weaviate_document_count']
            
    def add_error(self, error_type: str, collection_name: str, error_message: str):
        """Add an error to the report"""
        self.migration_data['errors'].append({
            'timestamp': datetime.now().isoformat(),
            'type': error_type,
            'collection': collection_name,
            'message': error_message
        })
        
    def generate_json_report(self) -> str:
        """Generate detailed JSON report"""
        filename = f"{self.reports_dir}/{self.migration_data['migration_id']}_detailed_report.json"
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(self.migration_data, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Detailed JSON report saved to: {filename}")
            return filename
            
        except Exception as e:
            logger.error(f"Failed to generate JSON report: {str(e)}")
            return None
            
    def generate_html_report(self) -> str:
        """Generate user-friendly HTML report"""
        filename = f"{self.reports_dir}/{self.migration_data['migration_id']}_report.html"
        
        try:
            html_content = self._create_html_content()
            
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(html_content)
                
            logger.info(f"HTML report saved to: {filename}")
            return filename
            
        except Exception as e:
            logger.error(f"Failed to generate HTML report: {str(e)}")
            return None
            
    def generate_summary_report(self) -> str:
        """Generate concise text summary report"""
        filename = f"{self.reports_dir}/{self.migration_data['migration_id']}_summary.txt"
        
        try:
            summary_content = self._create_summary_content()
            
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(summary_content)
                
            logger.info(f"Summary report saved to: {filename}")
            return filename
            
        except Exception as e:
            logger.error(f"Failed to generate summary report: {str(e)}")
            return None
            
    def _create_html_content(self) -> str:
        """Create HTML content for the report"""
        summary = self.migration_data['summary']
        collections = self.migration_data['collections']
        
        # Calculate success rate
        total_collections = summary['total_collections']
        success_rate = (summary['successful_collections'] / total_collections * 100) if total_collections > 0 else 0
        
        # Group collections by status
        successful_collections = [c for c in collections.values() if c['status'] == 'success']
        failed_collections = [c for c in collections.values() if c['status'] == 'failed']
        skipped_collections = [c for c in collections.values() if c['status'] == 'skipped']
        
        html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Weaviate to Zilliz Migration Report</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }}
        .container {{ max-width: 1200px; margin: 0 auto; background-color: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
        h1 {{ color: #333; text-align: center; border-bottom: 3px solid #4CAF50; padding-bottom: 10px; }}
        h2 {{ color: #555; margin-top: 30px; }}
        .summary {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin: 20px 0; }}
        .metric {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 20px; border-radius: 8px; text-align: center; }}
        .metric h3 {{ margin: 0; font-size: 2em; }}
        .metric p {{ margin: 5px 0 0 0; opacity: 0.9; }}
        .success {{ background: linear-gradient(135deg, #4CAF50 0%, #45a049 100%); }}
        .failed {{ background: linear-gradient(135deg, #f44336 0%, #da190b 100%); }}
        .skipped {{ background: linear-gradient(135deg, #ff9800 0%, #f57c00 100%); }}
        .total {{ background: linear-gradient(135deg, #2196F3 0%, #0b7dda 100%); }}
        .collection-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; margin: 20px 0; }}
        .collection-card {{ border: 1px solid #ddd; border-radius: 8px; padding: 15px; background-color: #fafafa; }}
        .collection-card h4 {{ margin: 0 0 10px 0; color: #333; }}
        .status-success {{ border-left: 5px solid #4CAF50; }}
        .status-failed {{ border-left: 5px solid #f44336; }}
        .status-skipped {{ border-left: 5px solid #ff9800; }}
        .info-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 10px; font-size: 0.9em; }}
        .info-item {{ display: flex; justify-content: space-between; padding: 5px; }}
        .info-item:nth-child(odd) {{ background-color: #f0f0f0; }}
        .error-section {{ background-color: #ffebee; border: 1px solid #ffcdd2; border-radius: 8px; padding: 15px; margin: 20px 0; }}
        .error-item {{ background-color: white; margin: 10px 0; padding: 10px; border-radius: 4px; border-left: 4px solid #f44336; }}
        .config-section {{ background-color: #e3f2fd; border: 1px solid #bbdefb; border-radius: 8px; padding: 15px; margin: 20px 0; }}
        table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
        th, td {{ border: 1px solid #ddd; padding: 12px; text-align: left; }}
        th {{ background-color: #f2f2f2; font-weight: bold; }}
        .duration {{ font-family: monospace; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>Weaviate to Zilliz Migration Report</h1>
        
        <div class="config-section">
            <h3>Migration Configuration</h3>
            <div class="info-grid">
                <div class="info-item"><span>Migration ID:</span><span>{self.migration_data['migration_id']}</span></div>
                <div class="info-item"><span>Start Time:</span><span>{self.migration_data.get('start_time', 'N/A')}</span></div>
                <div class="info-item"><span>End Time:</span><span>{self.migration_data.get('end_time', 'N/A')}</span></div>
                <div class="info-item"><span>Duration:</span><span class="duration">{self.migration_data.get('total_duration', 'N/A')}</span></div>
                <div class="info-item"><span>Batch Size:</span><span>{self.migration_data['configuration'].get('batch_size', 'N/A')}</span></div>
                <div class="info-item"><span>Weaviate Endpoint:</span><span>{self.migration_data['configuration'].get('weaviate_endpoint', 'N/A')}</span></div>
            </div>
        </div>
        
        <h2>Migration Summary</h2>
        <div class="summary">
            <div class="metric total">
                <h3>{summary['total_collections']}</h3>
                <p>Total Collections</p>
            </div>
            <div class="metric success">
                <h3>{summary['successful_collections']}</h3>
                <p>Successful</p>
            </div>
            <div class="metric failed">
                <h3>{summary['failed_collections']}</h3>
                <p>Failed</p>
            </div>
            <div class="metric skipped">
                <h3>{summary['skipped_collections']}</h3>
                <p>Skipped</p>
            </div>
        </div>
        
        <div class="summary">
            <div class="metric total">
                <h3>{summary['total_documents_processed']:,}</h3>
                <p>Documents Processed</p>
            </div>
            <div class="metric success">
                <h3>{summary['total_documents_migrated']:,}</h3>
                <p>Documents Migrated</p>
            </div>
            <div class="metric">
                <h3>{success_rate:.1f}%</h3>
                <p>Success Rate</p>
            </div>
        </div>
"""

        # Successful Collections
        if successful_collections:
            html += f"""
        <h2>Successful Collections ({len(successful_collections)})</h2>
        <div class="collection-grid">
"""
            for collection in successful_collections:
                schema_props = len(collection.get('schema', {}).get('properties', {}))
                html += f"""
            <div class="collection-card status-success">
                <h4>{collection['name']}</h4>
                <div class="info-grid">
                    <div class="info-item"><span>Weaviate Docs:</span><span>{collection['weaviate_document_count']:,}</span></div>
                    <div class="info-item"><span>Zilliz Docs:</span><span>{collection['zilliz_document_count']:,}</span></div>
                    <div class="info-item"><span>Schema Properties:</span><span>{schema_props}</span></div>
                    <div class="info-item"><span>Duration:</span><span class="duration">{collection.get('duration', 'N/A')}</span></div>
                    <div class="info-item"><span>Batches Processed:</span><span>{collection.get('batches_processed', 0)}</span></div>
                </div>
            </div>
"""
            html += "        </div>"

        # Failed Collections
        if failed_collections:
            html += f"""
        <h2>Failed Collections ({len(failed_collections)})</h2>
        <div class="collection-grid">
"""
            for collection in failed_collections:
                schema_props = len(collection.get('schema', {}).get('properties', {}))
                html += f"""
            <div class="collection-card status-failed">
                <h4>{collection['name']}</h4>
                <div class="info-grid">
                    <div class="info-item"><span>Weaviate Docs:</span><span>{collection['weaviate_document_count']:,}</span></div>
                    <div class="info-item"><span>Schema Properties:</span><span>{schema_props}</span></div>
                    <div class="info-item"><span>Duration:</span><span class="duration">{collection.get('duration', 'N/A')}</span></div>
                    <div class="info-item"><span>Batches Failed:</span><span>{collection.get('batches_failed', 0)}</span></div>
                </div>
                <p style="color: #d32f2f; margin-top: 10px; font-size: 0.9em;"><strong>Error:</strong> {collection.get('error_message', 'Unknown error')}</p>
            </div>
"""
            html += "        </div>"

        # Skipped Collections
        if skipped_collections:
            html += f"""
        <h2>Skipped Collections ({len(skipped_collections)})</h2>
        <div class="collection-grid">
"""
            for collection in skipped_collections:
                schema_props = len(collection.get('schema', {}).get('properties', {}))
                html += f"""
            <div class="collection-card status-skipped">
                <h4>{collection['name']}</h4>
                <div class="info-grid">
                    <div class="info-item"><span>Weaviate Docs:</span><span>{collection['weaviate_document_count']:,}</span></div>
                    <div class="info-item"><span>Schema Properties:</span><span>{schema_props}</span></div>
                    <div class="info-item"><span>Reason:</span><span>Already exists in Zilliz</span></div>
                </div>
            </div>
"""
            html += "        </div>"

        # Errors Section
        if self.migration_data['errors']:
            html += f"""
        <h2>Errors and Issues ({len(self.migration_data['errors'])})</h2>
        <div class="error-section">
"""
            for error in self.migration_data['errors']:
                html += f"""
            <div class="error-item">
                <strong>{error['type']}</strong> in collection <em>{error['collection']}</em><br>
                <small>{error['timestamp']}</small><br>
                {error['message']}
            </div>
"""
            html += "        </div>"

        # Collection Details Table
        html += """
        <h2>Detailed Collection Information</h2>
        <table>
            <thead>
                <tr>
                    <th>Collection Name</th>
                    <th>Status</th>
                    <th>Weaviate Docs</th>
                    <th>Zilliz Docs</th>
                    <th>Schema Properties</th>
                    <th>Duration</th>
                </tr>
            </thead>
            <tbody>
"""
        for collection in collections.values():
            schema_props = len(collection.get('schema', {}).get('properties', {}))
            status_color = {'success': '#4CAF50', 'failed': '#f44336', 'skipped': '#ff9800'}.get(collection['status'], '#666')
            html += f"""
                <tr>
                    <td>{collection['name']}</td>
                    <td style="color: {status_color}; font-weight: bold;">{collection['status'].upper()}</td>
                    <td>{collection['weaviate_document_count']:,}</td>
                    <td>{collection['zilliz_document_count']:,}</td>
                    <td>{schema_props}</td>
                    <td class="duration">{collection.get('duration', 'N/A')}</td>
                </tr>
"""
        
        html += """
            </tbody>
        </table>
        
        <div style="text-align: center; margin-top: 40px; color: #666; font-size: 0.9em;">
            Generated on """ + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + """
        </div>
    </div>
</body>
</html>"""
        
        return html
        
    def _create_summary_content(self) -> str:
        """Create text summary content"""
        summary = self.migration_data['summary']
        collections = self.migration_data['collections']
        
        content = f"""
WEAVIATE TO ZILLIZ MIGRATION SUMMARY REPORT
==========================================

Migration ID: {self.migration_data['migration_id']}
Start Time: {self.migration_data.get('start_time', 'N/A')}
End Time: {self.migration_data.get('end_time', 'N/A')}
Total Duration: {self.migration_data.get('total_duration', 'N/A')}

CONFIGURATION:
- Weaviate Endpoint: {self.migration_data['configuration'].get('weaviate_endpoint', 'N/A')}
- Zilliz URI: {self.migration_data['configuration'].get('zilliz_uri', 'N/A')}
- Batch Size: {self.migration_data['configuration'].get('batch_size', 'N/A')}
- Max Retries: {self.migration_data['configuration'].get('max_retries', 'N/A')}

OVERALL STATISTICS:
- Total Collections: {summary['total_collections']}
- Successful Collections: {summary['successful_collections']}
- Failed Collections: {summary['failed_collections']}
- Skipped Collections: {summary['skipped_collections']}
- Total Documents Processed: {summary['total_documents_processed']:,}
- Total Documents Migrated: {summary['total_documents_migrated']:,}
- Success Rate: {(summary['successful_collections'] / summary['total_collections'] * 100) if summary['total_collections'] > 0 else 0:.1f}%

COLLECTION DETAILS:
==================
"""
        
        # Successful collections
        successful_collections = [c for c in collections.values() if c['status'] == 'success']
        if successful_collections:
            content += f"\nSUCCESSFUL COLLECTIONS ({len(successful_collections)}):\n"
            for collection in successful_collections:
                schema_props = len(collection.get('schema', {}).get('properties', {}))
                content += f"✓ {collection['name']}\n"
                content += f"  - Weaviate Documents: {collection['weaviate_document_count']:,}\n"
                content += f"  - Zilliz Documents: {collection['zilliz_document_count']:,}\n"
                content += f"  - Schema Properties: {schema_props}\n"
                content += f"  - Duration: {collection.get('duration', 'N/A')}\n\n"
        
        # Failed collections
        failed_collections = [c for c in collections.values() if c['status'] == 'failed']
        if failed_collections:
            content += f"\nFAILED COLLECTIONS ({len(failed_collections)}):\n"
            for collection in failed_collections:
                schema_props = len(collection.get('schema', {}).get('properties', {}))
                content += f"✗ {collection['name']}\n"
                content += f"  - Weaviate Documents: {collection['weaviate_document_count']:,}\n"
                content += f"  - Schema Properties: {schema_props}\n"
                content += f"  - Duration: {collection.get('duration', 'N/A')}\n"
                content += f"  - Error: {collection.get('error_message', 'Unknown error')}\n\n"
        
        # Skipped collections
        skipped_collections = [c for c in collections.values() if c['status'] == 'skipped']
        if skipped_collections:
            content += f"\nSKIPPED COLLECTIONS ({len(skipped_collections)}):\n"
            for collection in skipped_collections:
                schema_props = len(collection.get('schema', {}).get('properties', {}))
                content += f"⏭ {collection['name']}\n"
                content += f"  - Weaviate Documents: {collection['weaviate_document_count']:,}\n"
                content += f"  - Schema Properties: {schema_props}\n"
                content += f"  - Reason: Already exists in Zilliz\n\n"
        
        # Errors
        if self.migration_data['errors']:
            content += f"\nERRORS AND ISSUES ({len(self.migration_data['errors'])}):\n"
            for error in self.migration_data['errors']:
                content += f"- {error['timestamp']}: {error['type']} in {error['collection']}\n"
                content += f"  {error['message']}\n\n"
        
        content += f"\nReport generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        
        return content
        
    def generate_all_reports(self) -> Dict[str, str]:
        """Generate all report formats and return their file paths"""
        reports = {}
        
        json_file = self.generate_json_report()
        if json_file:
            reports['json'] = json_file
            
        html_file = self.generate_html_report()
        if html_file:
            reports['html'] = html_file
            
        summary_file = self.generate_summary_report()
        if summary_file:
            reports['summary'] = summary_file
            
        return reports