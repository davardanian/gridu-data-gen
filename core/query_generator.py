"""
Query generator coordinator.

This module coordinates between SQL generation and AI responses.
"""

import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from typing import Optional, Dict, Any
from langfuse import observe
from core.ai_client import AIClient
from core.observability import observability


class QueryGenerator:
    """Coordinates query generation and AI responses"""
    
    def __init__(self):
        self.ai_client = AIClient()
    
    @observe(name="sql_query_generation")
    def generate_sql_query(self, natural_language_query: str, schema_info: Dict[str, Any]) -> Optional[str]:
        """Generate SQL query from natural language"""
        try:
            # Log the SQL generation attempt
            observability.log_ai_operation(
                "sql_generation_start",
                model="gemini-2.5-flash",
                query_length=len(natural_language_query),
                schema_tables=len(schema_info)
            )
            
            prompt = self._create_sql_generation_prompt(natural_language_query, schema_info)
            from config.settings import settings
            response = self.ai_client.generate_content(
                prompt, 
                temperature=0.1,
                max_tokens=settings.MAX_QUERY_GENERATION_TOKENS
            )
            
            if response:
                sql_query = self._extract_sql_from_response(response)
                if sql_query and self._validate_sql_against_schema(sql_query, schema_info):
                    observability.log_ai_operation(
                        "sql_generation_success",
                        model="gemini-2.5-flash",
                        sql_length=len(sql_query)
                    )
                    return sql_query
                elif sql_query:
                    observability.log_ai_operation(
                        "sql_generation_validation_failed",
                        model="gemini-2.5-flash",
                        reason="invalid_columns"
                    )
                    st.warning("Generated SQL query contains invalid columns. Trying alternative approach...")
                    return None
            else:
                observability.log_ai_operation(
                    "sql_generation_failed",
                    model="gemini-2.5-flash",
                    reason="no_response"
                )
            return None
            
        except Exception as e:
            observability.log_ai_operation(
                "sql_generation_error",
                model="gemini-2.5-flash",
                error=str(e)
            )
            observability.log_exception(e, "sql_query_generation")
            st.error(f"❌ SQL generation failed: {str(e)}")
            return None
    
    def _create_sql_generation_prompt(self, query: str, schema_info: Dict[str, Any]) -> str:
        """Create AI prompt for SQL generation"""
        prompt = f"""
You are an expert SQL query generator. Generate a PostgreSQL query for this request: "{query}"

Database Schema:
"""
        
        for table_name, table_info in schema_info.items():
            prompt += f"\nTable: {table_name}\n"
            prompt += "Columns:\n"
            for col in table_info['columns']:
                prompt += f"  - {col['name']}: {col['type']}"
                if not col.get('nullable', True):
                    prompt += " (NOT NULL)"
                prompt += "\n"
            
            if table_info.get('primary_key'):
                prompt += f"Primary Key: {table_info['primary_key']}\n"
            
            if table_info.get('foreign_keys'):
                prompt += "Foreign Keys:\n"
                for fk in table_info['foreign_keys']:
                    prompt += f"  - {fk['column']} -> {fk['referenced_table']}.{fk['referenced_column']}\n"
        
        prompt += f"""
CRITICAL INSTRUCTIONS:
1. Return ONLY the SQL query, no explanations
2. Use table and column names EXACTLY as shown in the schema above
3. Do NOT assume column names - only use what's explicitly listed
4. For "top N" queries, use ORDER BY with DESC and LIMIT
5. For "highest", "most expensive", "largest" use ORDER BY DESC
6. For "lowest", "cheapest", "smallest" use ORDER BY ASC
7. If a column doesn't exist in the schema, do NOT include it in the query
8. Ensure the query is safe (SELECT only, no DDL/DML operations)

Generate the SQL query:
"""
        
        return prompt
    
    def _extract_sql_from_response(self, ai_response: str) -> Optional[str]:
        """Extract SQL query from AI response"""
        try:
            # Remove markdown formatting if present
            response = ai_response.strip()
            if response.startswith('```sql'):
                response = response[6:]
            elif response.startswith('```'):
                response = response[3:]
            
            if response.endswith('```'):
                response = response[:-3]
            
            # Clean up the response
            response = response.strip()
            
            # Basic validation - check if it looks like SQL
            if any(keyword in response.upper() for keyword in ['SELECT', 'INSERT', 'UPDATE', 'DELETE']):
                return response
            
            return None
            
        except Exception as e:
            st.error(f"Error extracting SQL: {str(e)}")
            return None
    
    def _validate_sql_against_schema(self, sql_query: str, schema_info: Dict[str, Any]) -> bool:
        """Validate that SQL query only uses columns that exist in the schema"""
        try:
            import re
            
            # Extract table names from FROM clause
            from_match = re.search(r'FROM\s+(\w+)', sql_query, re.IGNORECASE)
            if not from_match:
                return False
            
            table_name = from_match.group(1).lower()
            
            # Check if table exists in schema
            if table_name not in schema_info:
                return False
            
            # Get available columns for this table
            available_columns = [col['name'].lower() for col in schema_info[table_name]['columns']]
            
            # Extract column names from SELECT clause
            select_match = re.search(r'SELECT\s+(.*?)\s+FROM', sql_query, re.IGNORECASE | re.DOTALL)
            if select_match:
                select_clause = select_match.group(1)
                
                # Handle * selection
                if '*' in select_clause:
                    return True
                
                # Extract individual column names, handling SQL aliases properly
                # Remove AS aliases first to avoid treating 'AS' as a column
                select_clause_clean = re.sub(r'\s+AS\s+\w+', '', select_clause, flags=re.IGNORECASE)
                
                # Extract column names, excluding SQL keywords and functions
                column_matches = re.findall(r'\b(\w+)\b', select_clause_clean)
                sql_keywords = {'COUNT', 'SUM', 'AVG', 'MAX', 'MIN', 'DISTINCT', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'AND', 'OR', 'NOT', 'IN', 'EXISTS', 'BETWEEN', 'LIKE', 'IS', 'NULL'}
                
                for column in column_matches:
                    if column.lower() not in available_columns and column.upper() not in sql_keywords:
                        st.warning(f"Column '{column}' does not exist in table '{table_name}'. Available columns: {', '.join(available_columns)}")
                        return False
            
            return True
            
        except Exception as e:
            st.error(f"Error validating SQL: {str(e)}")
            return False
    
    @observe(name="sql_query_execution")
    def execute_query(self, sql_query: str) -> Optional[pd.DataFrame]:
        """Execute SQL query against PostgreSQL database and return results"""
        try:
            from core.database_manager import DatabaseManager
            import time
            
            start_time = time.time()
            
            # Log query execution start
            observability.log_database_operation(
                "query_execution_start",
                query_length=len(sql_query),
                query_type="SELECT" if sql_query.strip().upper().startswith("SELECT") else "OTHER"
            )
            
            st.info("🔍 Executing SQL query against PostgreSQL database")
            
            db_manager = DatabaseManager()
            # Check if database is connected
            if not db_manager.is_connected():
                observability.log_database_operation(
                    "query_execution_failed",
                    reason="database_not_connected"
                )
                st.error("❌ Database not connected")
                return None
            
            # Execute query using database manager
            result = db_manager.execute_query(sql_query, ttl=0)  # No caching for real-time queries
            
            execution_time = time.time() - start_time
            
            if result is not None:
                observability.log_database_operation(
                    "query_execution_success",
                    rows_returned=len(result),
                    execution_time=execution_time,
                    columns_returned=len(result.columns) if hasattr(result, 'columns') else 0
                )
                observability.log_performance(
                    "sql_query_execution",
                    execution_time,
                    rows=len(result),
                    columns=len(result.columns) if hasattr(result, 'columns') else 0
                )
                st.success(f"✅ SQL query executed successfully - {len(result)} rows returned")
                return result
            else:
                observability.log_database_operation(
                    "query_execution_no_results",
                    execution_time=execution_time
                )
                st.error("❌ SQL query returned no results")
                return None
                
        except Exception as e:
            execution_time = time.time() - start_time if 'start_time' in locals() else 0
            observability.log_database_operation(
                "query_execution_error",
                error=str(e),
                execution_time=execution_time
            )
            observability.log_exception(e, "sql_query_execution")
            st.error(f"❌ Query execution failed: {str(e)}")
            return None
    
    def generate_ai_response(self, prompt: str, context: Dict[str, Any] = None) -> str:
        """Generate AI response as a string"""
        try:
            conversational_prompt = self._create_conversational_prompt(prompt, context or {})
            from config.settings import settings
            response = self.ai_client.generate_content(
                conversational_prompt, 
                temperature=0.7,
                max_tokens=settings.MAX_QUERY_GENERATION_TOKENS
            )
            
            if response:
                return response
            else:
                return "Error"
                
        except Exception as e:
            return "Error"
    
    def _create_conversational_prompt(self, user_query: str, context: Dict[str, Any]) -> str:
        """Create a conversational AI prompt with context"""
        prompt = f"""You are a helpful AI assistant that helps users analyze and understand their data. 
You have access to the following data context:

"""
        
        # Add data context if available
        if context.get('generated_tables'):
            prompt += "Available Tables:\n"
            for table_name, df in context['generated_tables'].items():
                prompt += f"- {table_name}: {len(df)} rows, {len(df.columns)} columns\n"
                prompt += f"  Columns: {', '.join(df.columns.tolist())}\n"
        
        if context.get('schema_info'):
            prompt += "\nDatabase Schema:\n"
            for table_name, table_info in context['schema_info'].items():
                prompt += f"- {table_name}:\n"
                for col in table_info.get('columns', []):
                    prompt += f"  - {col['name']}: {col['type']}\n"
        
        prompt += f"""
User Query: {user_query}

CRITICAL: Respond with ONLY 1-3 words maximum. Examples:
- "Done" 
- "Found 5 results"
- "Chart created"
- "No data"
- "Error occurred"

Do NOT provide explanations, suggestions, or multiple sentences. Just a minimal status response.

Response:"""
        
        return prompt
    
    def classify_query_type(self, query: str) -> str:
        """Classify the type of query for appropriate handling"""
        query_lower = query.lower()
        
        # Visualization keywords - check first as it's more specific
        viz_keywords = ['chart', 'graph', 'plot', 'visualize', 'visualization', 'show me', 'display', 'create a', 'make a', 'generate a', 'draw', 'based on', 'group by', 'distribution']
        if any(keyword in query_lower for keyword in viz_keywords):
            return "visualization"
        
        # SQL generation keywords
        sql_keywords = ['select', 'show', 'find', 'get', 'list', 'count', 'sum', 'average', 'max', 'min', 'top', 'highest', 'lowest']
        if any(keyword in query_lower for keyword in sql_keywords):
            return "sql_generation"
        
        # Data analysis keywords
        analysis_keywords = ['analyze', 'analysis', 'insights', 'summary', 'statistics', 'describe']
        if any(keyword in query_lower for keyword in analysis_keywords):
            return "data_analysis"
        
        return "general"
    
    @observe(name="visualization_query_generation")
    def generate_visualization_query(self, natural_language_query: str, schema_info: Dict[str, Any]) -> Optional[str]:
        """Generate SQL query for visualization from natural language"""
        try:
            # Log visualization query generation attempt
            observability.log_ai_operation(
                "visualization_query_generation_start",
                model="gemini-2.5-flash",
                query_length=len(natural_language_query),
                schema_tables=len(schema_info)
            )
            
            prompt = self._create_visualization_generation_prompt(natural_language_query, schema_info)
            from config.settings import settings
            response = self.ai_client.generate_content(
                prompt, 
                temperature=0.1,
                max_tokens=settings.MAX_QUERY_GENERATION_TOKENS
            )
            
            if response:
                sql_query = self._extract_sql_from_response(response)
                if sql_query and self._validate_sql_against_schema(sql_query, schema_info):
                    observability.log_ai_operation(
                        "visualization_query_generation_success",
                        model="gemini-2.5-flash",
                        sql_length=len(sql_query)
                    )
                    return sql_query
                elif sql_query:
                    observability.log_ai_operation(
                        "visualization_query_generation_validation_failed",
                        model="gemini-2.5-flash",
                        reason="invalid_columns"
                    )
                    st.warning("Generated visualization query contains invalid columns. Trying alternative approach...")
                    return None
            else:
                observability.log_ai_operation(
                    "visualization_query_generation_failed",
                    model="gemini-2.5-flash",
                    reason="no_response"
                )
            return None
            
        except Exception as e:
            observability.log_ai_operation(
                "visualization_query_generation_error",
                model="gemini-2.5-flash",
                error=str(e)
            )
            observability.log_exception(e, "visualization_query_generation")
            st.error(f"❌ Visualization query generation failed: {str(e)}")
            return None
    
    def _create_visualization_generation_prompt(self, query: str, schema_info: Dict[str, Any]) -> str:
        """Create AI prompt for visualization SQL generation"""
        prompt = f"""
You are an expert SQL query generator for data visualizations. Generate a PostgreSQL query that will fetch the data needed for this visualization request: "{query}"

Database Schema:
"""
        
        for table_name, table_info in schema_info.items():
            prompt += f"\nTable: {table_name}\n"
            prompt += "Columns:\n"
            for col in table_info['columns']:
                prompt += f"  - {col['name']}: {col['type']}"
                if not col.get('nullable', True):
                    prompt += " (NOT NULL)"
                prompt += "\n"
            
            if table_info.get('primary_key'):
                prompt += f"Primary Key: {table_info['primary_key']}\n"
            
            if table_info.get('foreign_keys'):
                prompt += "Foreign Keys:\n"
                for fk in table_info['foreign_keys']:
                    prompt += f"  - {fk['column']} -> {fk['referenced_table']}.{fk['referenced_column']}\n"
        
        prompt += f"""
CRITICAL INSTRUCTIONS FOR VISUALIZATION QUERIES:
1. Return ONLY the SQL query, no explanations
2. Use table and column names EXACTLY as shown in the schema above
3. Do NOT assume column names - only use what's explicitly listed
4. For visualizations, focus on getting the right data structure:
   - For bar charts: SELECT category_column, COUNT(*) AS count GROUP BY category_column ORDER BY count DESC
   - For line charts: SELECT date_column, numeric_column ORDER BY date_column
   - For scatter plots: SELECT x_column, y_column
   - For histograms: SELECT numeric_column (the query will handle binning)
   - For heatmaps: SELECT all relevant numeric columns for correlation
5. Use appropriate aggregations (COUNT, SUM, AVG, etc.) based on the visualization type
6. Add ORDER BY clauses for time series and categorical data
7. Use LIMIT for large datasets (e.g., LIMIT 1000)
8. Ensure the query is safe (SELECT only, no DDL/DML operations)
9. For "top N" visualizations, use ORDER BY with DESC and LIMIT
10. If a column doesn't exist in the schema, do NOT include it in the query
11. For date-based calculations (age, duration, etc.):
    - Use EXTRACT(YEAR FROM AGE(date_column)) for age calculations
    - Use CASE statements to group continuous data into meaningful ranges
    - Use COUNT(*) to get counts per group
12. IMPORTANT: Always use proper column aliases with AS keyword (e.g., COUNT(*) AS count, SUM(price) AS total_price)
13. For bar charts specifically:
    - Use GROUP BY with categorical columns
    - Use COUNT(*) AS count for frequency-based bar charts
    - Use SUM(numeric_column) AS total for sum-based bar charts
    - Always ORDER BY the aggregated column for better visualization

Generate the SQL query for visualization:
"""
        
        return prompt
    
    def create_visualization_from_query_results(self, query_results: pd.DataFrame, original_query: str) -> Optional[plt.Figure]:
        """Create visualization from SQL query results"""
        try:
            from utils.visualization import VisualizationManager
            
            viz_manager = VisualizationManager()
            
            # Determine chart type from original query
            chart_type = viz_manager.determine_chart_type(original_query, query_results)
            
            # Get appropriate columns for visualization
            numerical_cols = query_results.select_dtypes(include=['int64', 'float64']).columns
            categorical_cols = query_results.select_dtypes(include=['object', 'category']).columns
            
            # Determine x and y columns based on chart type and available data
            x_col = None
            y_col = None
            
            if chart_type in ["bar", "line"]:
                # For bar/line charts, use first categorical column as x, first numerical as y
                if len(categorical_cols) > 0:
                    x_col = categorical_cols[0]
                elif len(numerical_cols) > 0:
                    x_col = numerical_cols[0]
                
                if len(numerical_cols) > 0:
                    y_col = numerical_cols[0]
                    
            elif chart_type == "scatter":
                # For scatter plots, use first two numerical columns
                if len(numerical_cols) >= 2:
                    x_col = numerical_cols[0]
                    y_col = numerical_cols[1]
                elif len(numerical_cols) == 1:
                    x_col = numerical_cols[0]
                    
            elif chart_type == "histogram":
                # For histograms, use first numerical column
                if len(numerical_cols) > 0:
                    x_col = numerical_cols[0]
                    
            elif chart_type == "heatmap":
                # For heatmaps, use all numerical columns
                pass  # Will be handled in the chart creation
                
            elif chart_type == "box":
                # For box plots, use categorical as x, numerical as y
                if len(categorical_cols) > 0 and len(numerical_cols) > 0:
                    x_col = categorical_cols[0]
                    y_col = numerical_cols[0]
                elif len(numerical_cols) > 0:
                    x_col = numerical_cols[0]
            
            # Create the visualization
            chart = viz_manager.create_chart(query_results, chart_type, x_col, y_col)
            return chart
            
        except Exception as e:
            st.error(f"❌ Visualization creation failed: {str(e)}")
            return None

    def optimize_query(self, sql_query: str) -> str:
        """Optimize SQL query for better performance"""
        # Optimization suggestions
        optimized = sql_query
        
        # Add basic optimizations
        if 'SELECT *' in optimized.upper():
            optimized = optimized.replace('SELECT *', 'SELECT specific_columns')
        
        return optimized