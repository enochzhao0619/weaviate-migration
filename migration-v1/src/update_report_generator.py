#!/usr/bin/env python3
"""
PostgreSQL Datasets Update Report Generator
Generates comprehensive reports for PostgreSQL datasets update operations
"""

import json
import os
from datetime import datetime
from typing import Dict, List, Any, Optional
import logging

logger = logging.getLogger(__name__)


class UpdateReportGenerator:
    """Generates detailed reports for PostgreSQL datasets update operations"""
    
    def __init__(self, reports_dir: str = "reports"):
        self.reports_dir = reports_dir
        self.update_data = {
            'update_id': f"pg_update_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            'start_time': None,
            'end_time': None,
            'total_duration': None,
            'statistics': {},
            'configuration': {},
            'successful_updates': [],
            'failed_updates': [],
            'class_prefix_mismatches': [],
            'not_found_datasets': [],
            'errors': []
        }
        
        # Ensure reports directory exists
        os.makedirs(self.reports_dir, exist_ok=True)
        
    def set_update_start(self, start_time: datetime, config: Dict[str, Any]):
        """Set update start time and configuration"""
        self.update_data['start_time'] = start_time.isoformat()
        self.update_data['configuration'] = {
            'weaviate_endpoint': config.get('weaviate_endpoint', 'N/A'),
            'postgresql_host': config.get('postgresql_host', 'N/A'),
            'postgresql_database': config.get('postgresql_database', 'N/A'),
            'update_type': 'weaviate_to_milvus_vector_store_type'
        }
        
    def set_update_end(self, end_time: datetime):
        """Set update end time and calculate duration"""
        self.update_data['end_time'] = end_time.isoformat()
        if self.update_data['start_time']:
            start = datetime.fromisoformat(self.update_data['start_time'])
            duration = end_time - start
            self.update_data['total_duration'] = str(duration)
            
    def set_update_statistics(self, stats: Dict[str, Any]):
        """Set the complete update statistics"""
        self.update_data['statistics'] = stats
        
        # Process detailed information from stats
        if 'mismatch_details' in stats:
            self.update_data['class_prefix_mismatches'] = stats['mismatch_details']
            
        if 'errors' in stats:
            self.update_data['errors'] = [
                {
                    'timestamp': datetime.now().isoformat(),
                    'message': error,
                    'type': 'update_error'
                } for error in stats['errors']
            ]
            
    def add_successful_update(self, dataset_id: str, class_name: str, 
                            old_type: str = 'weaviate', new_type: str = 'milvus'):
        """Add a successful update record"""
        self.update_data['successful_updates'].append({
            'dataset_id': dataset_id,
            'class_name': class_name,
            'old_type': old_type,
            'new_type': new_type,
            'timestamp': datetime.now().isoformat()
        })
        
    def add_failed_update(self, dataset_id: str, class_name: str, error_message: str):
        """Add a failed update record"""
        self.update_data['failed_updates'].append({
            'dataset_id': dataset_id,
            'class_name': class_name,
            'error_message': error_message,
            'timestamp': datetime.now().isoformat()
        })
        
    def add_not_found_dataset(self, dataset_id: str, class_name: str):
        """Add a not found dataset record"""
        self.update_data['not_found_datasets'].append({
            'dataset_id': dataset_id,
            'class_name': class_name,
            'timestamp': datetime.now().isoformat()
        })
        
    def generate_json_report(self) -> str:
        """Generate detailed JSON report"""
        filename = f"{self.reports_dir}/{self.update_data['update_id']}_detailed_report.json"
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(self.update_data, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Detailed JSON report saved to: {filename}")
            return filename
            
        except Exception as e:
            logger.error(f"Failed to generate JSON report: {str(e)}")
            return None
            
    def generate_html_report(self) -> str:
        """Generate user-friendly HTML report"""
        filename = f"{self.reports_dir}/{self.update_data['update_id']}_report.html"
        
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
        filename = f"{self.reports_dir}/{self.update_data['update_id']}_summary.txt"
        
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
        stats = self.update_data['statistics']
        
        # Calculate success rate
        total_processed = stats.get('processed_datasets', 0)
        success_rate = (stats.get('updated_datasets', 0) / total_processed * 100) if total_processed > 0 else 0
        
        html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>PostgreSQL Datasets Update Report</title>
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
        .warning {{ background: linear-gradient(135deg, #ff9800 0%, #f57c00 100%); }}
        .total {{ background: linear-gradient(135deg, #2196F3 0%, #0b7dda 100%); }}
        .info {{ background: linear-gradient(135deg, #9c27b0 0%, #7b1fa2 100%); }}
        .config-section {{ background-color: #e3f2fd; border: 1px solid #bbdefb; border-radius: 8px; padding: 15px; margin: 20px 0; }}
        .info-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 10px; font-size: 0.9em; }}
        .info-item {{ display: flex; justify-content: space-between; padding: 5px; }}
        .info-item:nth-child(odd) {{ background-color: #f0f0f0; }}
        .error-section {{ background-color: #ffebee; border: 1px solid #ffcdd2; border-radius: 8px; padding: 15px; margin: 20px 0; }}
        .error-item {{ background-color: white; margin: 10px 0; padding: 10px; border-radius: 4px; border-left: 4px solid #f44336; }}
        .mismatch-section {{ background-color: #fff3e0; border: 1px solid #ffcc02; border-radius: 8px; padding: 15px; margin: 20px 0; }}
        .mismatch-item {{ background-color: white; margin: 10px 0; padding: 10px; border-radius: 4px; border-left: 4px solid #ff9800; }}
        table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
        th, td {{ border: 1px solid #ddd; padding: 12px; text-align: left; }}
        th {{ background-color: #f2f2f2; font-weight: bold; }}
        .duration {{ font-family: monospace; }}
        .status-success {{ color: #4CAF50; font-weight: bold; }}
        .status-failed {{ color: #f44336; font-weight: bold; }}
        .status-not-found {{ color: #ff9800; font-weight: bold; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>PostgreSQL Datasets Update Report</h1>
        
        <div class="config-section">
            <h3>Update Configuration</h3>
            <div class="info-grid">
                <div class="info-item"><span>Update ID:</span><span>{self.update_data['update_id']}</span></div>
                <div class="info-item"><span>Start Time:</span><span>{self.update_data.get('start_time', 'N/A')}</span></div>
                <div class="info-item"><span>End Time:</span><span>{self.update_data.get('end_time', 'N/A')}</span></div>
                <div class="info-item"><span>Duration:</span><span class="duration">{self.update_data.get('total_duration', 'N/A')}</span></div>
                <div class="info-item"><span>Update Type:</span><span>{self.update_data['configuration'].get('update_type', 'N/A')}</span></div>
                <div class="info-item"><span>Weaviate Endpoint:</span><span>{self.update_data['configuration'].get('weaviate_endpoint', 'N/A')}</span></div>
            </div>
        </div>
        
        <h2>Update Summary</h2>
        <div class="summary">
            <div class="metric total">
                <h3>{stats.get('total_weaviate_classes', 0)}</h3>
                <p>Total Weaviate Classes</p>
            </div>
            <div class="metric info">
                <h3>{stats.get('processed_datasets', 0)}</h3>
                <p>Processed Datasets</p>
            </div>
            <div class="metric success">
                <h3>{stats.get('updated_datasets', 0)}</h3>
                <p>Successfully Updated</p>
            </div>
            <div class="metric failed">
                <h3>{stats.get('failed_updates', 0)}</h3>
                <p>Failed Updates</p>
            </div>
        </div>
        
        <div class="summary">
            <div class="metric warning">
                <h3>{stats.get('not_found_datasets', 0)}</h3>
                <p>Not Found Datasets</p>
            </div>
            <div class="metric info">
                <h3>{stats.get('class_prefix_matches', 0)}</h3>
                <p>Class Prefix Matches</p>
            </div>
            <div class="metric warning">
                <h3>{stats.get('class_prefix_mismatches', 0)}</h3>
                <p>Class Prefix Mismatches</p>
            </div>
            <div class="metric">
                <h3>{success_rate:.1f}%</h3>
                <p>Success Rate</p>
            </div>
        </div>
"""

        # Successful Updates Section
        if len(self.update_data['successful_updates']) > 0:
            html += f"""
        <h2>Successful Updates ({len(self.update_data['successful_updates'])})</h2>
        <table>
            <thead>
                <tr>
                    <th>Dataset ID</th>
                    <th>Class Name</th>
                    <th>Type Change</th>
                    <th>Timestamp</th>
                </tr>
            </thead>
            <tbody>
"""
            for update in self.update_data['successful_updates']:
                html += f"""
                <tr>
                    <td>{update['dataset_id']}</td>
                    <td>{update['class_name']}</td>
                    <td><span class="status-success">{update['old_type']} → {update['new_type']}</span></td>
                    <td>{update['timestamp']}</td>
                </tr>
"""
            html += "            </tbody>\n        </table>"

        # Failed Updates Section
        if len(self.update_data['failed_updates']) > 0:
            html += f"""
        <h2>Failed Updates ({len(self.update_data['failed_updates'])})</h2>
        <table>
            <thead>
                <tr>
                    <th>Dataset ID</th>
                    <th>Class Name</th>
                    <th>Error Message</th>
                    <th>Timestamp</th>
                </tr>
            </thead>
            <tbody>
"""
            for update in self.update_data['failed_updates']:
                html += f"""
                <tr>
                    <td>{update['dataset_id']}</td>
                    <td>{update['class_name']}</td>
                    <td><span class="status-failed">{update['error_message']}</span></td>
                    <td>{update['timestamp']}</td>
                </tr>
"""
            html += "            </tbody>\n        </table>"

        # Not Found Datasets Section
        if len(self.update_data['not_found_datasets']) > 0:
            html += f"""
        <h2>Not Found Datasets ({len(self.update_data['not_found_datasets'])})</h2>
        <table>
            <thead>
                <tr>
                    <th>Dataset ID</th>
                    <th>Class Name</th>
                    <th>Timestamp</th>
                </tr>
            </thead>
            <tbody>
"""
            for dataset in self.update_data['not_found_datasets']:
                html += f"""
                <tr>
                    <td>{dataset['dataset_id']}</td>
                    <td>{dataset['class_name']}</td>
                    <td>{dataset['timestamp']}</td>
                </tr>
"""
            html += "            </tbody>\n        </table>"

        # Class Prefix Mismatches Section
        if len(self.update_data['class_prefix_mismatches']) > 0:
            html += f"""
        <h2>Class Prefix Mismatches ({len(self.update_data['class_prefix_mismatches'])})</h2>
        <div class="mismatch-section">
            <p>These datasets have class_prefix values that don't match their corresponding Weaviate class names:</p>
"""
            for mismatch in self.update_data['class_prefix_mismatches']:
                html += f"""
            <div class="mismatch-item">
                <strong>Dataset ID:</strong> {mismatch['dataset_id']}<br>
                <strong>Expected:</strong> {mismatch['class_name']}<br>
                <strong>Found:</strong> {mismatch['class_prefix']}
            </div>
"""
            html += "        </div>"

        # Errors Section
        if len(self.update_data['errors']) > 0:
            html += f"""
        <h2>Errors and Issues ({len(self.update_data['errors'])})</h2>
        <div class="error-section">
"""
            for error in self.update_data['errors']:
                html += f"""
            <div class="error-item">
                <strong>{error['type']}</strong><br>
                <small>{error['timestamp']}</small><br>
                {error['message']}
            </div>
"""
            html += "        </div>"

        html += f"""
        <div style="text-align: center; margin-top: 40px; color: #666; font-size: 0.9em;">
            Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        </div>
    </div>
</body>
</html>"""
        
        return html
        
    def _create_summary_content(self) -> str:
        """Create text summary content"""
        stats = self.update_data['statistics']
        
        content = f"""
POSTGRESQL DATASETS UPDATE SUMMARY REPORT
=========================================

Update ID: {self.update_data['update_id']}
Start Time: {self.update_data.get('start_time', 'N/A')}
End Time: {self.update_data.get('end_time', 'N/A')}
Total Duration: {self.update_data.get('total_duration', 'N/A')}

CONFIGURATION:
- Update Type: {self.update_data['configuration'].get('update_type', 'N/A')}
- Weaviate Endpoint: {self.update_data['configuration'].get('weaviate_endpoint', 'N/A')}
- PostgreSQL Host: {self.update_data['configuration'].get('postgresql_host', 'N/A')}
- PostgreSQL Database: {self.update_data['configuration'].get('postgresql_database', 'N/A')}

OVERALL STATISTICS:
- Total Weaviate Classes: {stats.get('total_weaviate_classes', 0)}
- Processed Datasets: {stats.get('processed_datasets', 0)}
- Successfully Updated: {stats.get('updated_datasets', 0)}
- Failed Updates: {stats.get('failed_updates', 0)}
- Not Found Datasets: {stats.get('not_found_datasets', 0)}
- Class Prefix Matches: {stats.get('class_prefix_matches', 0)}
- Class Prefix Mismatches: {stats.get('class_prefix_mismatches', 0)}
- Success Rate: {(stats.get('updated_datasets', 0) / stats.get('processed_datasets', 1) * 100):.1f}%

UPDATE DETAILS:
==============
"""
        
        # Successful updates
        if len(self.update_data['successful_updates']) > 0:
            content += f"\nSUCCESSFUL UPDATES ({len(self.update_data['successful_updates'])}):\n"
            for update in self.update_data['successful_updates']:
                content += f"✓ {update['class_name']} (Dataset: {update['dataset_id']})\n"
                content += f"  - Type changed: {update['old_type']} → {update['new_type']}\n"
                content += f"  - Timestamp: {update['timestamp']}\n\n"
        
        # Failed updates
        if len(self.update_data['failed_updates']) > 0:
            content += f"\nFAILED UPDATES ({len(self.update_data['failed_updates'])}):\n"
            for update in self.update_data['failed_updates']:
                content += f"✗ {update['class_name']} (Dataset: {update['dataset_id']})\n"
                content += f"  - Error: {update['error_message']}\n"
                content += f"  - Timestamp: {update['timestamp']}\n\n"
        
        # Not found datasets
        if len(self.update_data['not_found_datasets']) > 0:
            content += f"\nNOT FOUND DATASETS ({len(self.update_data['not_found_datasets'])}):\n"
            for dataset in self.update_data['not_found_datasets']:
                content += f"⚠ {dataset['class_name']} (Dataset: {dataset['dataset_id']})\n"
                content += f"  - Reason: Dataset not found in PostgreSQL database\n"
                content += f"  - Timestamp: {dataset['timestamp']}\n\n"
        
        # Class prefix mismatches
        if len(self.update_data['class_prefix_mismatches']) > 0:
            content += f"\nCLASS PREFIX MISMATCHES ({len(self.update_data['class_prefix_mismatches'])}):\n"
            for mismatch in self.update_data['class_prefix_mismatches']:
                content += f"⚠ Dataset: {mismatch['dataset_id']}\n"
                content += f"  - Expected: {mismatch['class_name']}\n"
                content += f"  - Found: {mismatch['class_prefix']}\n\n"
        
        # Errors
        if len(self.update_data['errors']) > 0:
            content += f"\nERRORS AND ISSUES ({len(self.update_data['errors'])}):\n"
            for error in self.update_data['errors']:
                content += f"- {error['timestamp']}: {error['type']}\n"
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