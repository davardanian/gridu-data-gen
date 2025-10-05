"""
Data Generation Orchestrator - High-level workflow manager.

This module orchestrates the entire data generation workflow:
1. Parses DDL schema files
2. Coordinates synthetic data generation
3. Manages database storage operations
4. Provides a unified interface for the Streamlit application
"""

import streamlit as st
import logging
import time
from typing import Dict, List, Optional
import pandas as pd
from langfuse import observe

from core.ddl_parser import DDLParser, Table
from core.synthetic_data_engine import SyntheticDataEngine
from core.database_manager import DatabaseManager
from core.observability import observability

logger = logging.getLogger(__name__)

class DataGenerationOrchestrator:
    """Orchestrates the complete data generation workflow from DDL to database storage"""
    
    def __init__(self):
        self.ddl_parser = DDLParser()
        self.data_engine = SyntheticDataEngine()
        self.db_manager = None
    
    @observe(name="data_generation_workflow")
    def generate_from_ddl(self, ddl_content: str, instructions: Optional[str] = None, 
                         temperature: float = 0.1, num_records: int = 100,
                         database_url: Optional[str] = None) -> Dict[str, pd.DataFrame]:
        """
        Generate synthetic data from PostgreSQL DDL content
        
        Args:
            ddl_content: DDL content as string
            instructions: Additional instructions for data generation
            temperature: Generation temperature (0.0 to 1.0)
            num_records: Number of records to generate per table
            database_url: Optional database URL for storing data
            
        Returns:
            Dictionary mapping table names to DataFrames
        """
        start_time = time.time()
        
        try:
            # Log workflow start
            observability.log_workflow_step(
                "data_generation_workflow",
                "start",
                "start",
                ddl_length=len(ddl_content),
                num_records=num_records,
                temperature=temperature,
                has_instructions=bool(instructions)
            )
            
            # Parse DDL to get table structures
            observability.log_workflow_step(
                "data_generation_workflow",
                "ddl_parsing",
                "start"
            )
            
            tables = self.ddl_parser.parse_ddl(ddl_content)
            
            if not tables:
                observability.log_workflow_step(
                    "data_generation_workflow",
                    "ddl_parsing",
                    "error",
                    reason="no_tables_found"
                )
                st.error("❌ No tables found in DDL")
                return {}
            
            observability.log_workflow_step(
                "data_generation_workflow",
                "ddl_parsing",
                "success",
                tables_parsed=len(tables)
            )
            st.success(f"✅ Parsed {len(tables)} tables from DDL")
            
            # Generate synthetic data
            observability.log_workflow_step(
                "data_generation_workflow",
                "synthetic_data_generation",
                "start",
                tables_count=len(tables),
                rows_per_table=num_records
            )
            
            generated_data = self.data_engine.generate_data(
                tables=tables,
                generation_prompt=instructions or "",
                temperature=temperature,
                rows_per_table=num_records
            )
            
            if not generated_data:
                observability.log_workflow_step(
                    "data_generation_workflow",
                    "synthetic_data_generation",
                    "error",
                    reason="no_data_generated"
                )
                st.error("❌ Failed to generate any data")
                return {}
            
            observability.log_workflow_step(
                "data_generation_workflow",
                "synthetic_data_generation",
                "success",
                tables_generated=len(generated_data)
            )
            st.success(f"✅ Generated data for {len(generated_data)} tables")
            
            # Store in database if URL provided
            if database_url:
                observability.log_workflow_step(
                    "data_generation_workflow",
                    "database_storage",
                    "start"
                )
                st.info("💾 Storing data in database...")
                self._store_data_in_database(ddl_content, generated_data, database_url)
                observability.log_workflow_step(
                    "data_generation_workflow",
                    "database_storage",
                    "success"
                )
            
            # Log workflow completion
            total_duration = time.time() - start_time
            observability.log_workflow_step(
                "data_generation_workflow",
                "complete",
                "success",
                total_duration=total_duration,
                tables_generated=len(generated_data),
                total_records=sum(len(df) for df in generated_data.values())
            )
            observability.log_performance(
                "complete_data_generation_workflow",
                total_duration,
                tables=len(generated_data),
                records_per_table=num_records
            )
            
            return generated_data
            
        except Exception as e:
            total_duration = time.time() - start_time
            observability.log_workflow_step(
                "data_generation_workflow",
                "error",
                "error",
                error=str(e),
                duration=total_duration
            )
            observability.log_exception(e, "data_generation_workflow")
            st.error(f"❌ Error in data generation: {str(e)}")
            logger.error(f"Error in data generation: {e}")
            return {}
    
    def _store_data_in_database(self, ddl_content: str, generated_data: Dict[str, pd.DataFrame], 
                               database_url: str):
        """Store generated data in database"""
        try:
            # Initialize database manager
            self.db_manager = DatabaseManager(database_url)
            
            # Execute DDL to create tables
            st.info("🏗️ Creating database tables...")
            ddl_success = self.db_manager.execute_ddl(ddl_content)
            
            if not ddl_success:
                st.error("❌ Failed to create database tables")
                return
            
            st.success("✅ Database tables created successfully")
            
            # Determine insertion order based on foreign key dependencies
            insertion_order = self.db_manager._get_insertion_order(generated_data, ddl_content)
            
            # Insert data
            st.info("📥 Inserting data into database...")
            results = self.db_manager.insert_dataframes(generated_data, insertion_order)
            
            # Report results
            successful_tables = [table for table, success in results.items() if success]
            failed_tables = [table for table, success in results.items() if not success]
            
            if successful_tables:
                st.success(f"✅ Successfully inserted data for {len(successful_tables)} tables: {', '.join(successful_tables)}")
            
            if failed_tables:
                st.error(f"❌ Failed to insert data for {len(failed_tables)} tables: {', '.join(failed_tables)}")
            
        except Exception as e:
            st.error(f"❌ Error storing data in database: {str(e)}")
            logger.error(f"Error storing data in database: {e}")
        finally:
            if self.db_manager:
                self.db_manager.close()
    
    def get_table_preview(self, generated_data: Dict[str, pd.DataFrame], 
                         table_name: str, num_rows: int = 5) -> Optional[pd.DataFrame]:
        """Get a preview of generated data for a specific table"""
        if table_name not in generated_data:
            return None
        
        df = generated_data[table_name]
        return df.head(num_rows)
    
    def get_generation_summary(self, generated_data: Dict[str, pd.DataFrame]) -> Dict[str, int]:
        """Get summary of generated data"""
        return {table_name: len(df) for table_name, df in generated_data.items()}

