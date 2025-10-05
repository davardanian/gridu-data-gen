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
    
    
    def execute_ddl(self, ddl_content: str, drop_existing: bool = True) -> bool:
        """
        Execute DDL statements to create tables
        
        Args:
            ddl_content: DDL statements to execute
            drop_existing: Whether to drop existing tables first
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Use SQLAlchemy connection
            return self._execute_ddl_sqlalchemy(ddl_content, drop_existing=drop_existing)
                
        except Exception as e:
            logger.error(f"DDL execution failed: {e}")
            st.error(f"❌ Failed to create database tables: {str(e)}")
            return False
    
    def create_tables_from_ddl(self, ddl_content: str, drop_existing: bool = True) -> bool:
        """
        Create tables from DDL content with proper drop_existing handling
        
        Args:
            ddl_content: DDL statements to execute
            drop_existing: Whether to drop existing tables first
            
        Returns:
            True if successful, False otherwise
        """
        return self.execute_ddl(ddl_content, drop_existing=drop_existing)
    
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
            # First, try to clear any existing data from the tables
            self._clear_existing_data(list(generated_data.keys()))
            
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
    
    def _clear_existing_data(self, table_names: List[str]) -> None:
        """Clear existing data from tables to prevent duplicate key violations"""
        try:
            with self.engine.connect() as conn:
                for table_name in table_names:
                    try:
                        # Clear all data from the table
                        conn.execute(text(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE"))
                        logger.info(f"Cleared existing data from table: {table_name}")
                    except Exception as e:
                        logger.warning(f"Could not clear data from table {table_name}: {e}")
                        # Try DELETE as fallback
                        try:
                            conn.execute(text(f"DELETE FROM {table_name}"))
                            logger.info(f"Deleted existing data from table: {table_name}")
                        except Exception as delete_e:
                            logger.warning(f"Could not delete data from table {table_name}: {delete_e}")
                
                conn.commit()
                logger.info("Committed data clearing operations")
                
        except Exception as e:
            logger.warning(f"Error clearing existing data: {e}")
    
    def validate_dataframe(self, table_name: str, df: pd.DataFrame) -> tuple[bool, List[str]]:
        """
        Validate DataFrame data against database schema constraints
        
        Args:
            table_name: Name of the target table
            df: DataFrame to validate
            
        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        errors = []
        
        try:
            # Get table schema information
            schema_info = self._get_table_schema(table_name)
            if not schema_info:
                errors.append(f"Could not retrieve schema for table {table_name}")
                return False, errors
            
            # Validate each row
            for idx, row in df.iterrows():
                row_errors = self._validate_row_data(table_name, row, schema_info, idx)
                errors.extend(row_errors)
            
            # Check for duplicate primary keys
            pk_errors = self._validate_primary_keys(table_name, df, schema_info)
            errors.extend(pk_errors)
            
            # Check for duplicate unique constraints
            unique_errors = self._validate_unique_constraints(table_name, df, schema_info)
            errors.extend(unique_errors)
            
            is_valid = len(errors) == 0
            if not is_valid:
                logger.warning(f"Data validation failed for table {table_name}: {len(errors)} errors")
                for error in errors[:5]:  # Log first 5 errors
                    logger.warning(f"  - {error}")
                if len(errors) > 5:
                    logger.warning(f"  - ... and {len(errors) - 5} more errors")
            
            return is_valid, errors
            
        except Exception as e:
            error_msg = f"Error validating data for table {table_name}: {e}"
            logger.error(error_msg)
            return False, [error_msg]
    
    def _get_table_schema(self, table_name: str) -> Optional[Dict]:
        """Get table schema information from database"""
        try:
            with self.engine.connect() as conn:
                # Get column information
                columns_query = """
                SELECT 
                    column_name,
                    data_type,
                    character_maximum_length,
                    is_nullable,
                    column_default
                FROM information_schema.columns 
                WHERE table_name = :table_name
                ORDER BY ordinal_position
                """
                
                result = conn.execute(text(columns_query), {"table_name": table_name})
                columns = result.fetchall()
                
                # Get primary key information
                pk_query = """
                SELECT kcu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu 
                    ON tc.constraint_name = kcu.constraint_name
                WHERE tc.table_name = :table_name 
                    AND tc.constraint_type = 'PRIMARY KEY'
                """
                
                pk_result = conn.execute(text(pk_query), {"table_name": table_name})
                primary_keys = [row[0] for row in pk_result.fetchall()]
                
                # Get foreign key information
                fk_query = """
                SELECT 
                    kcu.column_name,
                    ccu.table_name AS foreign_table_name,
                    ccu.column_name AS foreign_column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu 
                    ON tc.constraint_name = kcu.constraint_name
                JOIN information_schema.constraint_column_usage ccu 
                    ON ccu.constraint_name = tc.constraint_name
                WHERE tc.table_name = :table_name 
                    AND tc.constraint_type = 'FOREIGN KEY'
                """
                
                fk_result = conn.execute(text(fk_query), {"table_name": table_name})
                foreign_keys = {row[0]: {"table": row[1], "column": row[2]} for row in fk_result.fetchall()}
                
                return {
                    "columns": {col[0]: {
                        "type": col[1],
                        "max_length": col[2],
                        "nullable": col[3] == "YES",
                        "default": col[4]
                    } for col in columns},
                    "primary_keys": primary_keys,
                    "foreign_keys": foreign_keys
                }
                
        except Exception as e:
            logger.error(f"Error getting schema for table {table_name}: {e}")
            return None
    
    def _validate_row_data(self, table_name: str, row: pd.Series, schema_info: Dict, row_idx: int) -> List[str]:
        """Validate a single row of data"""
        errors = []
        columns = schema_info["columns"]
        
        for column_name, value in row.items():
            if column_name not in columns:
                continue
                
            column_info = columns[column_name]
            
            # Check null constraints
            if pd.isna(value) and not column_info["nullable"]:
                errors.append(f"Row {row_idx}: Column '{column_name}' cannot be null")
                continue
            
            if pd.isna(value):
                continue  # Skip validation for null values
            
            # Check string length constraints
            if column_info["max_length"] and isinstance(value, str):
                if len(value) > column_info["max_length"]:
                    errors.append(f"Row {row_idx}: Column '{column_name}' value too long ({len(value)} > {column_info['max_length']}): '{value[:50]}...'")
            
            # Check date validity
            if column_info["type"] in ["date", "timestamp", "timestamp with time zone"]:
                if isinstance(value, str):
                    try:
                        pd.to_datetime(value)
                    except:
                        errors.append(f"Row {row_idx}: Column '{column_name}' invalid date: '{value}'")
        
        return errors
    
    def _validate_primary_keys(self, table_name: str, df: pd.DataFrame, schema_info: Dict) -> List[str]:
        """Validate primary key constraints"""
        errors = []
        primary_keys = schema_info["primary_keys"]
        
        if not primary_keys:
            return errors
        
        # Check for duplicate primary key values
        pk_columns = [col for col in primary_keys if col in df.columns]
        if pk_columns:
            duplicates = df.duplicated(subset=pk_columns, keep=False)
            if duplicates.any():
                duplicate_rows = df[duplicates][pk_columns].drop_duplicates()
                for _, row in duplicate_rows.iterrows():
                    pk_values = ", ".join([f"{col}={row[col]}" for col in pk_columns])
                    errors.append(f"Duplicate primary key: {pk_values}")
        
        return errors
    
    def _validate_unique_constraints(self, table_name: str, df: pd.DataFrame, schema_info: Dict) -> List[str]:
        """Validate unique constraints (including unique indexes)"""
        errors = []
        
        try:
            with self.engine.connect() as conn:
                # Get unique constraints
                unique_query = """
                SELECT 
                    kcu.column_name,
                    tc.constraint_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu 
                    ON tc.constraint_name = kcu.constraint_name
                WHERE tc.table_name = :table_name 
                    AND tc.constraint_type = 'UNIQUE'
                """
                
                result = conn.execute(text(unique_query), {"table_name": table_name})
                unique_constraints = result.fetchall()
                
                # Check for duplicate values in unique columns
                for column_name, constraint_name in unique_constraints:
                    if column_name in df.columns:
                        duplicates = df.duplicated(subset=[column_name], keep=False)
                        if duplicates.any():
                            duplicate_values = df[duplicates][column_name].drop_duplicates()
                            for value in duplicate_values:
                                errors.append(f"Duplicate unique value in column '{column_name}': '{value}'")
                
                # Also check for unique indexes
                index_query = """
                SELECT 
                    a.attname as column_name,
                    i.relname as index_name
                FROM pg_class t
                JOIN pg_index ix ON t.oid = ix.indrelid
                JOIN pg_class i ON i.oid = ix.indexrelid
                JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
                WHERE t.relname = :table_name
                    AND ix.indisunique = true
                    AND i.relname NOT LIKE '%_pkey'
                """
                
                index_result = conn.execute(text(index_query), {"table_name": table_name})
                unique_indexes = index_result.fetchall()
                
                # Check for duplicate values in unique index columns
                for column_name, index_name in unique_indexes:
                    if column_name in df.columns:
                        duplicates = df.duplicated(subset=[column_name], keep=False)
                        if duplicates.any():
                            duplicate_values = df[duplicates][column_name].drop_duplicates()
                            for value in duplicate_values:
                                errors.append(f"Duplicate unique index value in column '{column_name}': '{value}'")
                
        except Exception as e:
            logger.warning(f"Could not validate unique constraints for table {table_name}: {e}")
        
        return errors
    
    def clean_dataframe(self, table_name: str, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean DataFrame data to fix common issues before validation
        
        Args:
            table_name: Name of the target table
            df: DataFrame to clean
            
        Returns:
            Cleaned DataFrame
        """
        try:
            # Get table schema information
            schema_info = self._get_table_schema(table_name)
            if not schema_info:
                logger.warning(f"Could not get schema for table {table_name}, skipping data cleaning")
                return df
            
            cleaned_df = df.copy()
            columns = schema_info["columns"]
            
            for column_name in cleaned_df.columns:
                if column_name not in columns:
                    continue
                    
                column_info = columns[column_name]
                
                # Fix string length issues
                if column_info["max_length"] and column_info["type"] in ["character varying", "varchar", "text"]:
                    cleaned_df[column_name] = cleaned_df[column_name].astype(str).apply(
                        lambda x: x[:column_info["max_length"]] if len(str(x)) > column_info["max_length"] else x
                    )
                
                # Fix date issues
                if column_info["type"] in ["date", "timestamp", "timestamp with time zone"]:
                    cleaned_df[column_name] = cleaned_df[column_name].apply(
                        lambda x: self._fix_invalid_date(x) if pd.notna(x) else x
                    )
                
                # Fix null constraint issues
                if not column_info["nullable"]:
                    # Replace null values with appropriate defaults
                    if column_info["type"] in ["character varying", "varchar", "text"]:
                        cleaned_df[column_name] = cleaned_df[column_name].fillna("")
                    elif column_info["type"] in ["integer", "bigint", "smallint"]:
                        cleaned_df[column_name] = cleaned_df[column_name].fillna(0)
                    elif column_info["type"] in ["numeric", "decimal", "real", "double precision"]:
                        cleaned_df[column_name] = cleaned_df[column_name].fillna(0.0)
                    elif column_info["type"] in ["boolean"]:
                        cleaned_df[column_name] = cleaned_df[column_name].fillna(False)
                    elif column_info["type"] in ["date", "timestamp", "timestamp with time zone"]:
                        cleaned_df[column_name] = cleaned_df[column_name].fillna("1900-01-01")
            
            # Fix duplicate unique values
            cleaned_df = self._fix_duplicate_unique_values(table_name, cleaned_df, schema_info)
            
            logger.info(f"Cleaned data for table {table_name}")
            return cleaned_df
            
        except Exception as e:
            logger.warning(f"Error cleaning data for table {table_name}: {e}")
            return df
    
    def _fix_invalid_date(self, date_value) -> str:
        """Fix invalid dates like Feb 29 in non-leap years"""
        try:
            if pd.isna(date_value):
                return "1900-01-01"
            
            date_str = str(date_value)
            
            # Check for Feb 29 in non-leap years
            if "-02-29" in date_str:
                year = int(date_str.split("-")[0])
                if not self._is_leap_year(year):
                    # Change to Feb 28
                    fixed_date = date_str.replace("-02-29", "-02-28")
                    logger.warning(f"Fixed invalid date {date_str} to {fixed_date}")
                    return fixed_date
            
            # Validate the date
            pd.to_datetime(date_str)
            return date_str
            
        except Exception as e:
            logger.warning(f"Could not fix date {date_value}: {e}")
            return "1900-01-01"
    
    def _is_leap_year(self, year: int) -> bool:
        """Check if a year is a leap year"""
        return year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)
    
    def _fix_duplicate_unique_values(self, table_name: str, df: pd.DataFrame, schema_info: Dict) -> pd.DataFrame:
        """Fix duplicate unique values by generating new unique values"""
        try:
            with self.engine.connect() as conn:
                # Get unique constraints
                unique_query = """
                SELECT 
                    kcu.column_name,
                    tc.constraint_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu 
                    ON tc.constraint_name = kcu.constraint_name
                WHERE tc.table_name = :table_name 
                    AND tc.constraint_type = 'UNIQUE'
                """
                
                result = conn.execute(text(unique_query), {"table_name": table_name})
                unique_constraints = result.fetchall()
                
                cleaned_df = df.copy()
                
                # Fix duplicate values in unique columns
                for column_name, constraint_name in unique_constraints:
                    if column_name in cleaned_df.columns:
                        # Find duplicates
                        duplicates = cleaned_df.duplicated(subset=[column_name], keep=False)
                        if duplicates.any():
                            logger.warning(f"Found duplicate values in unique column '{column_name}' for table '{table_name}'")
                            
                            # Get existing values from database
                            existing_query = f"SELECT DISTINCT {column_name} FROM {table_name}"
                            existing_result = conn.execute(text(existing_query))
                            existing_values = set(row[0] for row in existing_result.fetchall())
                            
                            # Fix duplicates by generating new unique values
                            for idx in cleaned_df[duplicates].index:
                                original_value = cleaned_df.loc[idx, column_name]
                                new_value = self._generate_unique_value(original_value, existing_values, column_name)
                                cleaned_df.loc[idx, column_name] = new_value
                                existing_values.add(new_value)
                                logger.info(f"Fixed duplicate value '{original_value}' to '{new_value}' in column '{column_name}'")
                
        except Exception as e:
            logger.warning(f"Error fixing duplicate unique values for table {table_name}: {e}")
            return df
        
        return cleaned_df
    
    def _generate_unique_value(self, original_value, existing_values: set, column_name: str) -> str:
        """Generate a unique value based on the original value"""
        import random
        import string
        
        if column_name.lower() == 'isbn':
            # For ISBN, generate a new valid ISBN
            return self._generate_unique_isbn(existing_values)
        elif column_name.lower() in ['email', 'username']:
            # For email/username, add a random suffix
            base = str(original_value).split('@')[0] if '@' in str(original_value) else str(original_value)
            suffix = ''.join(random.choices(string.digits, k=4))
            return f"{base}_{suffix}@{'example.com' if '@' in str(original_value) else ''}"
        else:
            # For other fields, add a random suffix
            suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=4))
            return f"{original_value}_{suffix}"
    
    def _generate_unique_isbn(self, existing_values: set) -> str:
        """Generate a unique ISBN"""
        import random
        
        while True:
            # Generate a random ISBN-13
            isbn = f"978-{random.randint(100000000, 999999999)}"
            if isbn not in existing_values:
                return isbn
    
    def _execute_ddl_sqlalchemy(self, ddl_content: str, drop_existing: bool = True) -> bool:
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
                
                # Drop existing tables if requested
                if drop_existing:
                    logger.info(f"Dropping existing tables: {table_names}")
                    for table_name in reversed(table_names):
                        try:
                            # First, drop any dependent objects
                            conn.execute(text(f"DROP TABLE IF EXISTS {table_name} CASCADE"))
                            logger.info(f"Dropped existing table: {table_name}")
                            
                            # Also try to drop any sequences that might be left behind
                            try:
                                conn.execute(text(f"DROP SEQUENCE IF EXISTS {table_name}_id_seq CASCADE"))
                                conn.execute(text(f"DROP SEQUENCE IF EXISTS {table_name}_seq CASCADE"))
                                conn.execute(text(f"DROP SEQUENCE IF EXISTS {table_name}_pk_seq CASCADE"))
                            except Exception as seq_e:
                                logger.debug(f"Could not drop sequences for {table_name}: {seq_e}")
                                
                        except Exception as e:
                            logger.warning(f"Could not drop table {table_name}: {e}")
                    
                    # Commit the drops before creating new tables
                    conn.commit()
                    logger.info("Committed table drops")
                
                # Execute DDL statements
                for statement in statements:
                    if statement.strip():
                        conn.execute(text(statement))
                        logger.info(f"Executed statement: {statement[:100]}...")
                
                # Reset sequences for primary key columns after table creation
                if drop_existing:
                    logger.info(f"Resetting sequences for tables: {table_names}")
                    self._reset_sequences(conn, table_names)
                else:
                    logger.info("Skipping sequence reset because drop_existing=False")
                
                conn.commit()
                return True
                
        except Exception as e:
            logger.error(f"SQLAlchemy DDL execution failed: {e}")
            return False
    
    def _reset_sequences(self, conn, table_names: List[str]) -> None:
        """Reset sequences for primary key columns to start from 1"""
        try:
            logger.info(f"Starting sequence reset for {len(table_names)} tables")
            for table_name in table_names:
                logger.info(f"Processing table: {table_name}")
                
                # First, try to find all sequences associated with this table
                sequences_query = """
                SELECT 
                    sequence_name,
                    column_name
                FROM information_schema.sequences s
                JOIN information_schema.columns c ON s.sequence_name LIKE '%' || c.table_name || '%'
                WHERE c.table_name = :table_name
                AND c.column_default LIKE '%' || s.sequence_name || '%'
                """
                
                try:
                    result = conn.execute(text(sequences_query), {"table_name": table_name})
                    sequences = result.fetchall()
                    
                    for seq_name, col_name in sequences:
                        try:
                            conn.execute(text(f"ALTER SEQUENCE {seq_name} RESTART WITH 1"))
                            logger.info(f"Reset sequence {seq_name} for {table_name}.{col_name} to start from 1")
                        except Exception as e:
                            logger.warning(f"Could not reset sequence {seq_name}: {e}")
                            
                except Exception as e:
                    logger.warning(f"Could not query sequences for table {table_name}: {e}")
                
                # Fallback: try common sequence naming patterns
                common_patterns = [
                    f"{table_name}_id_seq",
                    f"{table_name}_seq",
                    f"{table_name}_pk_seq"
                ]
                
                for seq_name in common_patterns:
                    try:
                        # Check if sequence exists
                        seq_check = conn.execute(text("""
                            SELECT 1 FROM pg_sequences WHERE sequencename = :seq_name
                        """), {"seq_name": seq_name})
                        
                        if seq_check.fetchone():
                            conn.execute(text(f"ALTER SEQUENCE {seq_name} RESTART WITH 1"))
                            logger.info(f"Reset sequence {seq_name} to start from 1")
                    except Exception as e:
                        continue
                        
        except Exception as e:
            logger.error(f"Error resetting sequences: {e}")
            # Don't let sequence reset failure prevent table creation
    
    def verify_schema_match(self, schema_info: Dict) -> Dict[str, Dict]:
        """
        Verify that database schema matches the expected schema
        
        Args:
            schema_info: Dictionary with table schemas
            
        Returns:
            Dictionary with verification results for each table
        """
        results = {}
        
        try:
            with self.engine.connect() as conn:
                for table_name, expected_schema in schema_info.items():
                    try:
                        # Check if table exists
                        table_check = conn.execute(text("""
                            SELECT 1 FROM information_schema.tables 
                            WHERE table_name = %s
                        """), (table_name,))
                        
                        if not table_check.fetchone():
                            results[table_name] = {
                                'status': 'missing',
                                'message': f'Table {table_name} does not exist in database'
                            }
                            continue
                        
                        # Get actual columns
                        columns_query = conn.execute(text("""
                            SELECT column_name, data_type, is_nullable
                            FROM information_schema.columns
                            WHERE table_name = %s
                            ORDER BY ordinal_position
                        """), (table_name,))
                        
                        actual_columns = {row[0]: {'type': row[1], 'nullable': row[2] == 'YES'} 
                                        for row in columns_query.fetchall()}
                        
                        # Compare with expected schema
                        expected_columns = {col['name']: col for col in expected_schema['columns']}
                        
                        missing_columns = set(expected_columns.keys()) - set(actual_columns.keys())
                        extra_columns = set(actual_columns.keys()) - set(expected_columns.keys())
                        
                        if missing_columns or extra_columns:
                            results[table_name] = {
                                'status': 'mismatch',
                                'missing_columns': list(missing_columns),
                                'extra_columns': list(extra_columns)
                            }
                        else:
                            results[table_name] = {
                                'status': 'match',
                                'message': f'Table {table_name} schema matches expected schema'
                            }
                            
                    except Exception as e:
                        results[table_name] = {
                            'status': 'error',
                            'message': f'Error verifying table {table_name}: {str(e)}'
                        }
                        
        except Exception as e:
            logger.error(f"Error in schema verification: {e}")
            return {table_name: {'status': 'error', 'message': str(e)} 
                   for table_name in schema_info.keys()}
        
        return results
    
    def insert_dataframe(self, table_name: str, df: pd.DataFrame) -> bool:
        """
        Insert DataFrame into database table with validation
        
        Args:
            table_name: Name of the target table
            df: DataFrame to insert
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Clean data first to fix common issues
            cleaned_df = self.clean_dataframe(table_name, df)
            
            # Validate data before insertion
            is_valid, validation_errors = self.validate_dataframe(table_name, cleaned_df)
            
            if not is_valid:
                logger.error(f"Data validation failed for table {table_name}")
                st.error(f"❌ **Data Validation Failed for {table_name}**")
                st.error(f"Found {len(validation_errors)} validation errors:")
                
                # Show first 5 errors to user
                for error in validation_errors[:5]:
                    st.error(f"  • {error}")
                
                if len(validation_errors) > 5:
                    st.error(f"  • ... and {len(validation_errors) - 5} more errors")
                
                st.error("**Solution:** The AI will regenerate data with proper validation.")
                return False
            
            # Use cleaned data for insertion
            df = cleaned_df
            
            # Use SQLAlchemy connection
            df.to_sql(table_name, self.engine, if_exists='append', index=False, method='multi')
            
            logger.info(f"Successfully inserted {len(df)} rows into table: {table_name}")
            return True
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Failed to insert data into table {table_name}: {e}")
            
            # Provide more helpful error messages for common issues
            if "duplicate key value violates unique constraint" in error_msg:
                st.error(f"❌ **Primary Key Conflict in {table_name}**")
                st.error("The table already contains data with the same primary key values.")
                st.error("**Solution:** Try running the data generation again - the system will now properly drop and recreate tables.")
                st.error("**Technical Details:** " + error_msg)
            elif "foreign key constraint" in error_msg:
                st.error(f"❌ **Foreign Key Constraint Violation in {table_name}**")
                st.error("The data references non-existent records in related tables.")
                st.error("**Solution:** Ensure all referenced tables are created and populated first.")
                st.error("**Technical Details:** " + error_msg)
            elif "value too long for type" in error_msg:
                st.error(f"❌ **Data Too Long for Column in {table_name}**")
                st.error("Some data exceeds the maximum length allowed by the database schema.")
                st.error("**Solution:** The system will now validate data before insertion to prevent this.")
                st.error("**Technical Details:** " + error_msg)
            elif "date/time field value out of range" in error_msg:
                st.error(f"❌ **Invalid Date in {table_name}**")
                st.error("Some dates are invalid (e.g., February 29th in non-leap years).")
                st.error("**Solution:** The system will now validate dates before insertion.")
                st.error("**Technical Details:** " + error_msg)
            else:
                st.error(f"❌ Failed to insert data into {table_name}: {error_msg}")
            
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
