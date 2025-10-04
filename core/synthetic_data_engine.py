"""
Synthetic Data Engine - AI-powered data generation core.

This module handles the core synthetic data generation using AI models:
1. Creates prompts for AI data generation
2. Processes AI responses and converts to DataFrames
3. Validates and fixes generated data
4. Provides fallback data generation when AI fails
"""

import json
import pandas as pd
from typing import Dict, List, Any, Optional
import logging
import random
from datetime import datetime, timedelta
from core.ddl_parser import Table, Column, DataType
from core.ai_client import AIClient

logger = logging.getLogger(__name__)

class SyntheticDataEngine:
    """AI-powered engine for generating realistic synthetic data from table schemas"""
    
    def __init__(self):
        self.ai_client = AIClient()
    
    def generate_data(self, tables: List[Table], generation_prompt: str = "", 
                     temperature: float = 0.3, rows_per_table: int = 50) -> Dict[str, pd.DataFrame]:
        """
        Generate synthetic data for multiple tables
        
        Args:
            tables: List of Table objects from DDL parser
            generation_prompt: Additional instructions for data generation
            temperature: Generation temperature (0.0 to 1.0)
            rows_per_table: Number of rows to generate per table
            
        Returns:
            Dictionary mapping table names to DataFrames
        """
        try:
            generated_data = {}
            
            # Generate data for each table
            for table in tables:
                logger.info(f"Generating data for table: {table.name}")
                
                # Generate data for this table
                table_data = self._generate_table_data(
                    table, generation_prompt, temperature, rows_per_table
                )
                
                if table_data is not None and not table_data.empty:
                    generated_data[table.name] = table_data
                    logger.info(f"Successfully generated {len(table_data)} rows for table: {table.name}")
                else:
                    logger.error(f"Failed to generate data for table: {table.name}")
                    # Generate fallback data
                    fallback_data = self._generate_fallback_data(table, rows_per_table)
                    if fallback_data is not None and not fallback_data.empty:
                        generated_data[table.name] = fallback_data
                        logger.info(f"Generated fallback data for table: {table.name}")
            
            return generated_data
            
        except Exception as e:
            logger.error(f"Error generating synthetic data: {e}")
            return {}
    
    def _generate_table_data(self, table: Table, generation_prompt: str, 
                           temperature: float, rows_per_table: int) -> Optional[pd.DataFrame]:
        """Generate data for a single table using AI"""
        try:
            # Create prompt for data generation
            prompt = self._create_data_generation_prompt(
                table, generation_prompt, rows_per_table
            )
            
            # Generate data using AI
            from config.settings import settings
            response = self.ai_client.generate_content(
                prompt,
                temperature=temperature,
                max_tokens=settings.MAX_DATA_GENERATION_TOKENS
            )
            
            if not response:
                logger.error(f"No response received for table: {table.name}")
                return None
            
            # Parse JSON response
            cleaned_text = self._clean_json_response(response)
            data_json = json.loads(cleaned_text)
            
            # Convert to DataFrame
            df = pd.DataFrame(data_json)
            
            # Validate and fix DataFrame
            df = self._validate_and_fix_dataframe(df, table)
            
            return df
            
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error for table {table.name}: {e}")
            return None
        except Exception as e:
            logger.error(f"Error generating data for table {table.name}: {e}")
            return None
    
    def _create_data_generation_prompt(self, table: Table, generation_prompt: str, rows_per_table: int) -> str:
        """Create a prompt for data generation"""
        prompt_parts = [
            f"Generate {rows_per_table} realistic rows of data for a PostgreSQL table named '{table.name}'.",
            "",
            "Table Schema:",
            f"Table: {table.name}",
            "Columns:"
        ]
        
        for column in table.columns:
            col_info = f"  - {column.name}: {column.data_type.value}"
            if column.length:
                col_info += f"({column.length})"
            if column.precision and column.scale:
                col_info += f"({column.precision},{column.scale})"
            if not column.nullable:
                col_info += " NOT NULL"
            if column.default_value:
                col_info += f" DEFAULT {column.default_value}"
            if column.is_primary_key:
                col_info += " (PRIMARY KEY)"
            if column.is_foreign_key:
                col_info += f" (FOREIGN KEY -> {column.foreign_table}.{column.foreign_column})"
            
            prompt_parts.append(col_info)
        
        prompt_parts.extend([
            "",
            "CRITICAL REQUIREMENTS:",
            "- NEVER use null, NULL, None, NONE, undefined, or empty values",
            "- EVERY field must contain realistic, meaningful data",
            "- Use proper data formats (YYYY-MM-DD for dates, realistic emails, etc.)",
            "- For SERIAL columns (auto-increment), DO NOT include them in the generated data",
            "- For PRIMARY KEY columns (like user_id, product_id, category_id), ALWAYS include them with UNIQUE sequential integers starting from 1",
            "- For foreign key columns, use sequential IDs starting from 1",
            "- Ensure all NOT NULL columns have valid values",
            "- Each row must have a UNIQUE primary key value",
            "- IMPORTANT: Include ALL columns in the JSON output, including primary keys",
            "- CRITICAL: Use DIVERSE, REALISTIC names and data - avoid repetitive patterns like 'John Doe', 'Jane Smith'",
            "- Generate varied, realistic data that represents different demographics and backgrounds",
            "",
            "Return the data as a JSON array of objects.",
            "Example format (DO NOT use these exact values - create your own diverse data):",
            '[{"user_id": 1, "name": "Alex", "email": "alex@example.com"}, {"user_id": 2, "name": "Sam", "email": "sam@example.com"}]',
            "",
            "IMPORTANT: The example above is just to show the JSON structure. DO NOT copy the names 'Alex' or 'Sam' or any other values from this example. Generate completely different, diverse, realistic data for your actual output."
        ])
        
        if generation_prompt:
            prompt_parts.extend([
                "Additional Instructions:",
                generation_prompt,
                ""
            ])
        
        return "\n".join(prompt_parts)
    
    def _clean_json_response(self, response_text: str) -> str:
        """Clean JSON response from AI model"""
        # Remove any markdown formatting
        if response_text.startswith('```json'):
            response_text = response_text[7:]
        if response_text.endswith('```'):
            response_text = response_text[:-3]
        
        # Remove any leading/trailing whitespace
        response_text = response_text.strip()
        
        return response_text
    
    def _validate_and_fix_dataframe(self, df: pd.DataFrame, table: Table) -> pd.DataFrame:
        """Validate and fix DataFrame data"""
        try:
            # Add missing columns (excluding SERIAL columns from AI generation)
            expected_columns = [col.name for col in table.columns if col.data_type not in [DataType.SERIAL, DataType.BIGSERIAL]]
            missing_columns = set(expected_columns) - set(df.columns)
            
            for col_name in missing_columns:
                df[col_name] = self._get_default_value_for_column(table, col_name)
            
            # Add SERIAL columns for preview purposes (with sequential IDs)
            for column in table.columns:
                if column.data_type in [DataType.SERIAL, DataType.BIGSERIAL] and column.name not in df.columns:
                    df[column.name] = range(1, len(df) + 1)
            
            # Fix NOT NULL columns
            for column in table.columns:
                if not column.nullable and column.name in df.columns:
                    null_mask = df[column.name].isnull()
                    if null_mask.any():
                        df.loc[null_mask, column.name] = self._get_default_value_for_column(table, column.name)
            
            return df
            
        except Exception as e:
            logger.error(f"Error validating DataFrame: {e}")
            return df
    
    def _get_default_value_for_column(self, table: Table, column_name: str) -> Any:
        """Get a default value for a column"""
        # Find the column definition
        column = None
        for col in table.columns:
            if col.name == column_name:
                column = col
                break
        
        if not column:
            return None
        
        # Generate appropriate default value based on data type
        if column.data_type in [DataType.SERIAL, DataType.BIGSERIAL]:
            return 1  # Default sequential ID for SERIAL columns
        elif column.data_type == DataType.INTEGER:
            return random.randint(1, 1000)
        elif column.data_type == DataType.BIGINT:
            return random.randint(1, 1000000)
        elif column.data_type in [DataType.VARCHAR, DataType.TEXT]:
            if 'email' in column_name.lower():
                return f"user{random.randint(1, 1000)}@example.com"
            else:
                return f"default_{column_name}_{random.randint(1, 100)}"
        elif column.data_type == DataType.BOOLEAN:
            return random.choice([True, False])
        elif column.data_type == DataType.DATE:
            return datetime.now().date().isoformat()
        elif column.data_type == DataType.TIMESTAMP:
            return datetime.now().isoformat()
        elif column.data_type in [DataType.DECIMAL, DataType.NUMERIC]:
            return round(random.uniform(0, 100), 2)
        else:
            return f"default_{column_name}"
    
    def _generate_fallback_data(self, table: Table, rows_per_table: int) -> Optional[pd.DataFrame]:
        """Generate fallback data when AI generation fails"""
        try:
            data = {}
            
            for column in table.columns:
                column_data = []
                for i in range(rows_per_table):
                    if column.data_type in [DataType.SERIAL, DataType.BIGSERIAL]:
                        # Generate sequential IDs for SERIAL columns
                        value = i + 1
                    elif column.data_type == DataType.INTEGER:
                        value = random.randint(1, 1000)
                    elif column.data_type == DataType.BIGINT:
                        value = random.randint(1, 1000000)
                    elif column.data_type in [DataType.VARCHAR, DataType.TEXT]:
                        if 'email' in column.name.lower():
                            value = f"user{i+1}@example.com"
                        else:
                            value = f"{column.name}_{i+1}"
                    elif column.data_type == DataType.BOOLEAN:
                        value = random.choice([True, False])
                    elif column.data_type == DataType.DATE:
                        value = (datetime.now() - timedelta(days=random.randint(0, 365))).date().isoformat()
                    elif column.data_type == DataType.TIMESTAMP:
                        value = (datetime.now() - timedelta(days=random.randint(0, 365))).isoformat()
                    elif column.data_type in [DataType.DECIMAL, DataType.NUMERIC]:
                        value = round(random.uniform(0, 100), 2)
                    else:
                        value = f"{column.name}_{i+1}"
                    
                    column_data.append(value)
                
                data[column.name] = column_data
            
            return pd.DataFrame(data)
            
        except Exception as e:
            logger.error(f"Error generating fallback data for table {table.name}: {e}")
            return None

