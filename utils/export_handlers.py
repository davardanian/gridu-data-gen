# utils/export_handlers.py
import pandas as pd
import zipfile
import io
from typing import Dict, Any, Optional
from core.observability import observability

class ExportManager:
    """Manages data export functionality"""
    
    def __init__(self):
        self.observability = observability
    
    def create_csv_export(self, data_dict: Dict[str, pd.DataFrame]) -> bytes:
        """Create CSV export of all tables"""
        try:
            self.observability.log_info("Creating CSV export", tables=len(data_dict))
            
            # Create a single CSV with all tables
            csv_content = ""
            
            for table_name, df in data_dict.items():
                # Add table header
                csv_content += f"\n=== TABLE: {table_name} ===\n"
                
                # Add table data
                csv_content += df.to_csv(index=False)
                csv_content += "\n"
            
            return csv_content.encode('utf-8')
            
        except Exception as e:
            self.observability.log_error(f"CSV export failed: {str(e)}")
            raise e
    
    def create_zip_export(self, data_dict: Dict[str, pd.DataFrame]) -> bytes:
        """Create ZIP archive of all tables as separate CSV files"""
        try:
            self.observability.log_info("Creating ZIP export", tables=len(data_dict))
            
            # Create ZIP file in memory
            zip_buffer = io.BytesIO()
            
            with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                for table_name, df in data_dict.items():
                    # Create CSV content for this table
                    csv_content = df.to_csv(index=False)
                    
                    # Add to ZIP file
                    zip_file.writestr(f"{table_name}.csv", csv_content)
            
            zip_buffer.seek(0)
            return zip_buffer.getvalue()
            
        except Exception as e:
            self.observability.log_error(f"ZIP export failed: {str(e)}")
            raise e
    
    def create_individual_csv(self, table_name: str, df: pd.DataFrame) -> bytes:
        """Create CSV for a single table"""
        try:
            csv_content = df.to_csv(index=False)
            return csv_content.encode('utf-8')
            
        except Exception as e:
            self.observability.log_error(f"Individual CSV export failed for {table_name}: {str(e)}")
            raise e
    
    def get_export_summary(self, data_dict: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """Get summary of export data"""
        try:
            summary = {
                "total_tables": len(data_dict),
                "total_records": sum(len(df) for df in data_dict.values()),
                "total_size_bytes": 0,
                "tables": {}
            }
            
            for table_name, df in data_dict.items():
                table_summary = {
                    "records": len(df),
                    "columns": len(df.columns),
                    "size_bytes": len(df.to_csv(index=False).encode('utf-8'))
                }
                summary["tables"][table_name] = table_summary
                summary["total_size_bytes"] += table_summary["size_bytes"]
            
            return summary
            
        except Exception as e:
            self.observability.log_error(f"Export summary failed: {str(e)}")
            return {"error": str(e)}
