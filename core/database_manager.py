"""
Database Manager for PostgreSQL operations.

This module provides a clean, focused approach to database operations
without the complexity and over-engineering of the previous implementation.
"""

import logging
from typing import Dict, List, Optional
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text, MetaData, Table as SQLTable
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)

class DatabaseManager:
    """Database manager for PostgreSQL operations"""
    
    def __init__(self, database_url: Optional[str] = None):
        """
        Initialize database manager
        
        Args:
            database_url: Database connection URL. If None, uses DATABASE_URL from environment
        """
        if database_url is None:
            from config.settings import Settings
            self.database_url = Settings.DATABASE_URL
        else:
            self.database_url = database_url
            
        self.engine = None
        self.SessionLocal = None
        self.conn = None
        
        self._initialize_connection()
    
    def _initialize_connection(self):
        """Initialize database connection using SQLAlchemy"""
        try:
            # Always use SQLAlchemy with DATABASE_URL from environment
            self.engine = create_engine(self.database_url)
            self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
            self._test_connection()
            
            logger.info("Database connection established successfully")
            
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise
    
    def _test_connection(self):
        """Test SQLAlchemy connection"""
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            raise
    
    
    def execute_ddl(self, ddl_content: str) -> bool:
        """
        Execute DDL statements to create tables
        
        Args:
            ddl_content: DDL statements to execute
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Use SQLAlchemy connection
            return self._execute_ddl_sqlalchemy(ddl_content)
                
        except Exception as e:
            logger.error(f"DDL execution failed: {e}")
            st.error(f"❌ Failed to create database tables: {str(e)}")
            return False
    
    def create_tables_from_ddl(self, ddl_content: str, drop_existing: bool = True) -> bool:
        """
        Create tables from DDL content (alias for execute_ddl for compatibility)
        
        Args:
            ddl_content: DDL statements to execute
            drop_existing: Whether to drop existing tables first
            
        Returns:
            True if successful, False otherwise
        """
        return self.execute_ddl(ddl_content)
    
    def store_generated_data(self, generated_data: Dict[str, pd.DataFrame], ddl_content: str = None) -> Dict[str, bool]:
        """
        Store generated data in database tables
        
        Args:
            generated_data: Dictionary mapping table names to DataFrames
            ddl_content: Optional DDL content for dependency ordering
            
        Returns:
            Dictionary mapping table names to success status
        """
        try:
            # Insertion order - tables without foreign keys first
            insertion_order = self._get_insertion_order(generated_data, ddl_content)
            
            # Insert data
            results = self.insert_dataframes(generated_data, insertion_order)
            
            # Log results
            successful = sum(1 for success in results.values() if success)
            total = len(results)
            logger.info(f"Data insertion completed: {successful}/{total} tables successful")
            
            return results
            
        except Exception as e:
            logger.error(f"Failed to store generated data: {e}")
            st.error(f"❌ Failed to store generated data: {str(e)}")
            return {table_name: False for table_name in generated_data.keys()}
    
    
    def _execute_ddl_sqlalchemy(self, ddl_content: str) -> bool:
        """Execute DDL using SQLAlchemy connection"""
        try:
            with self.engine.connect() as conn:
                # Parse statements
                statements = self._parse_ddl_statements(ddl_content)
                
                # Get table names to drop first
                table_names = []
                for statement in statements:
                    if statement.upper().startswith('CREATE TABLE'):
                        table_name = self._extract_table_name(statement)
                        if table_name:
                            table_names.append(table_name)
                
                # Drop existing tables
                for table_name in reversed(table_names):
                    try:
                        conn.execute(text(f"DROP TABLE IF EXISTS {table_name} CASCADE"))
                        logger.info(f"Dropped existing table: {table_name}")
                    except Exception as e:
                        logger.warning(f"Could not drop table {table_name}: {e}")
                
                # Execute DDL statements
                for statement in statements:
                    if statement.strip():
                        conn.execute(text(statement))
                        logger.info(f"Executed statement: {statement[:100]}...")
                
                conn.commit()
                return True
                
        except Exception as e:
            logger.error(f"SQLAlchemy DDL execution failed: {e}")
            return False
    
    def insert_dataframe(self, table_name: str, df: pd.DataFrame) -> bool:
        """
        Insert DataFrame into database table
        
        Args:
            table_name: Name of the target table
            df: DataFrame to insert
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Use SQLAlchemy connection
            df.to_sql(table_name, self.engine, if_exists='append', index=False, method='multi')
            
            logger.info(f"Successfully inserted {len(df)} rows into table: {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to insert data into table {table_name}: {e}")
            st.error(f"❌ Failed to insert data into {table_name}: {str(e)}")
            return False
    
    def insert_dataframes(self, dataframes: Dict[str, pd.DataFrame], 
                         insertion_order: Optional[List[str]] = None) -> Dict[str, bool]:
        """
        Insert multiple DataFrames into their respective tables
        
        Args:
            dataframes: Dictionary mapping table names to DataFrames
            insertion_order: Optional order for table insertion
            
        Returns:
            Dictionary mapping table names to success status
        """
        results = {}
        
        # Use provided order or default to alphabetical
        if insertion_order:
            table_names = insertion_order
        else:
            table_names = sorted(dataframes.keys())
        
        for table_name in table_names:
            if table_name in dataframes:
                df = dataframes[table_name]
                success = self.insert_dataframe(table_name, df)
                results[table_name] = success
            else:
                logger.warning(f"Table {table_name} not found in dataframes")
                results[table_name] = False
        
        return results
    
    def execute_query(self, query: str, ttl: int = 600) -> Optional[pd.DataFrame]:
        """Execute SQL query and return results"""
        try:
            # Use SQLAlchemy connection
            with self.engine.connect() as conn:
                result = pd.read_sql(query, conn)
                return result
                    
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            st.error(f"❌ Query execution failed: {str(e)}")
            return None
    
    def is_connected(self) -> bool:
        """Check if database is connected"""
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception:
            return False
    
    def _parse_ddl_statements(self, ddl_content: str) -> List[str]:
        """Parse DDL content into individual statements"""
        import re
        
        # Remove comments
        ddl_content = re.sub(r'--.*$', '', ddl_content, flags=re.MULTILINE)
        ddl_content = re.sub(r'/\*.*?\*/', '', ddl_content, flags=re.DOTALL)
        
        # Normalize whitespace
        ddl_content = re.sub(r'\s+', ' ', ddl_content)
        ddl_content = ddl_content.strip()
        
        # Split by semicolon and filter out empty statements
        statements = [stmt.strip() for stmt in ddl_content.split(';') if stmt.strip()]
        
        return statements
    
    def _extract_table_name(self, create_table_statement: str) -> Optional[str]:
        """Extract table name from CREATE TABLE statement"""
        import re
        match = re.search(r'CREATE\s+TABLE\s+(\w+)', create_table_statement, re.IGNORECASE)
        return match.group(1) if match else None
    
    def _get_insertion_order(self, generated_data: Dict[str, pd.DataFrame], ddl_content: str = None) -> List[str]:
        """Get insertion order for tables based on foreign key dependencies"""
        try:
            if not ddl_content:
                # No DDL provided, use alphabetical order
                return sorted(generated_data.keys())
            
            # Parse DDL to extract foreign key relationships
            from core.ddl_parser import DDLParser
            parser = DDLParser()
            tables = parser.parse_ddl(ddl_content)
            
            # Build dependency graph
            dependencies = {}  # table -> list of tables it depends on
            all_tables = set(generated_data.keys())
            
            for table in tables:
                if table.name in all_tables:
                    dependencies[table.name] = []
                    for fk_column, fk_table, fk_column_ref in table.foreign_keys:
                        if fk_table in all_tables:
                            dependencies[table.name].append(fk_table)
            
            # Topological sort to get insertion order
            insertion_order = self._topological_sort(dependencies)
            
            # Add any tables not in DDL (shouldn't happen, but safety)
            for table_name in all_tables:
                if table_name not in insertion_order:
                    insertion_order.append(table_name)
            
            logger.info(f"Insertion order determined: {insertion_order}")
            return insertion_order
            
        except Exception as e:
            logger.warning(f"Could not determine insertion order: {e}")
            return sorted(generated_data.keys())
    
    def _topological_sort(self, dependencies: Dict[str, List[str]]) -> List[str]:
        """Topological sort to determine insertion order based on dependencies"""
        # Kahn's algorithm for topological sorting
        in_degree = {table: 0 for table in dependencies}
        
        # Calculate in-degrees
        for table, deps in dependencies.items():
            for dep in deps:
                if dep in in_degree:
                    in_degree[table] += 1
        
        # Find nodes with no incoming edges
        queue = [table for table, degree in in_degree.items() if degree == 0]
        result = []
        
        while queue:
            current = queue.pop(0)
            result.append(current)
            
            # Remove current node and update in-degrees
            for table, deps in dependencies.items():
                if current in deps:
                    in_degree[table] -= 1
                    if in_degree[table] == 0:
                        queue.append(table)
        
        return result
    
    def close(self):
        """Close database connections"""
        try:
            if self.engine:
                self.engine.dispose()
            logger.info("Database connections closed")
        except Exception as e:
            logger.error(f"Error closing database connections: {e}")
