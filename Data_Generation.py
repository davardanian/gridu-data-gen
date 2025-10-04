import streamlit as st
import pandas as pd
from datetime import datetime
import time
from core.auth_manager import auth_manager
from core.data_generation_orchestrator import DataGenerationOrchestrator
from core.guardrails import GuardrailsManager
from core.observability import observability
from utils.export_handlers import ExportManager
from utils.visualization import VisualizationManager
from config.settings import settings

# Initialize managers
guardrails = GuardrailsManager()
export_manager = ExportManager()
viz_manager = VisualizationManager()

# Log application startup
observability.log_info("🚀 Streamlit application starting")

# Helper functions

def process_table_edit(table_name, edit_prompt):
    """Process table edit request using AI"""
    try:
        # Get the current table data
        if table_name not in st.session_state["generated_tables"]:
            st.error(f"Table '{table_name}' not found in generated data.")
            return None
        
        original_df = st.session_state["generated_tables"][table_name]
        
        # Identify which columns are ID columns that should be preserved
        id_columns = [col for col in original_df.columns if col.endswith('_id') and original_df[col].dtype == 'int64']
        
        # Create AI prompt for data modification
        ai_prompt = f"""
You are a data modification expert. I have a table called '{table_name}' with the following structure and data:

Table Structure:
{original_df.dtypes.to_string()}

EXACT COLUMN ORDER (CRITICAL - MUST FOLLOW THIS ORDER):
{list(original_df.columns)}

Total Rows: {len(original_df)}
Current Data (first 5 rows as sample):
{original_df.head().to_string()}

User Request: {edit_prompt}

Please modify the data according to the user's request. Return the modified data as a CSV format that I can parse back into a pandas DataFrame. 

CRITICAL REQUIREMENTS:
1. The CSV header row MUST be: {','.join(original_df.columns)}
2. Each data row MUST have values in the EXACT same order as the columns above
3. MUST keep exactly {len(original_df)} rows (same as original) unless user specifically requests to change the number of rows
4. The sample above shows only the first 5 rows - you need to generate {len(original_df)} total rows
5. Ensure data consistency and realism across all {len(original_df)} rows
6. Return ONLY the CSV data, no explanations or markdown formatting
7. Generate diverse, realistic data for all {len(original_df)} rows
8. CRITICAL: If any field contains commas, semicolons, or quotes, you MUST wrap the entire field in double quotes
9. For comma-separated lists (like awards, genres), wrap the entire field in quotes: "Award 1, Award 2, Award 3"

SPECIAL COLUMN HANDLING:
- ID COLUMNS TO PRESERVE: {id_columns} - Keep these EXACTLY as they are in the original data (sequential integers 1, 2, 3, etc.)
- MODIFYABLE COLUMNS: All other columns can be modified according to the user's request

DATA TYPE REQUIREMENTS:
- For ID columns (like author_id, book_id, etc.): Use sequential integers starting from 1 (PRESERVE EXISTING VALUES)
- For first_name, last_name, middle_name: Use string values (names)
- For DATE columns: Use YYYY-MM-DD format
- For TIMESTAMP columns: Use YYYY-MM-DD HH:MM:SS format
- For BOOLEAN columns: Use True/False (capitalized)
- For DECIMAL columns: Use proper decimal notation (e.g., 19.99)
- For TEXT columns: Use string values

EXAMPLE FORMAT (first row only):
{','.join(original_df.columns)}
{','.join([str(original_df.iloc[0][col]) for col in original_df.columns])}

IMPORTANT CSV FORMATTING EXAMPLE:
If you have fields with commas, quotes, or special characters, format them like this:
author_id,first_name,last_name,awards,genres
1,"John","Doe","Award 1, Award 2, Award 3","Genre 1, Genre 2, Genre 3"
2,"Jane","Smith","Single Award","Single Genre"

Modified CSV data:
"""
        
        # Initialize AI client and process the request
        from core.ai_client import AIClient
        ai_client = AIClient()
        
        with st.spinner(f"🤖 AI is modifying {table_name}..."):
            response = ai_client.generate_content(ai_prompt, temperature=0.3)
        
        if not response:
            st.error("❌ AI modification failed. Please try again.")
            return None
        
        # Parse the AI response as CSV
        try:
            import io
            import pandas as pd
            
            # Clean the response (remove any markdown formatting)
            csv_data = response.strip()
            if csv_data.startswith('```'):
                # Remove markdown code blocks
                lines = csv_data.split('\n')
                csv_data = '\n'.join([line for line in lines if not line.startswith('```')])
            
            # Parse CSV with error handling and proper quoting
            try:
                modified_df = pd.read_csv(io.StringIO(csv_data), quotechar='"', escapechar='\\')
            except pd.errors.ParserError as csv_error:
                st.warning(f"⚠️ CSV parsing error: {csv_error}. Attempting to fix...")
                # Try with more flexible parsing
                try:
                    modified_df = pd.read_csv(io.StringIO(csv_data), quotechar='"', escapechar='\\', on_bad_lines='skip')
                except Exception as e2:
                    st.error(f"❌ Could not parse CSV even with flexible parsing: {e2}")
                    # Show the problematic data for debugging
                    st.error("Problematic CSV data:")
                    st.code(csv_data[:1000] + "..." if len(csv_data) > 1000 else csv_data)
                    return None
            
            # Validate that the structure matches
            if list(modified_df.columns) != list(original_df.columns):
                st.warning("⚠️ Column structure changed. Attempting to fix...")
                # Try to align columns
                for col in original_df.columns:
                    if col not in modified_df.columns:
                        modified_df[col] = original_df[col]
                modified_df = modified_df[original_df.columns]
            
            # Additional validation: Check if ID columns are preserved correctly
            for col in original_df.columns:
                if col.endswith('_id') and original_df[col].dtype == 'int64':
                    # Check if the first value in this column is actually an integer
                    try:
                        first_val = modified_df[col].iloc[0]
                        int(first_val)
                        
                        # Check if ID values are preserved (should be sequential 1, 2, 3, etc.)
                        original_ids = original_df[col].tolist()
                        modified_ids = modified_df[col].tolist()
                        
                        if original_ids != modified_ids:
                            st.warning(f"⚠️ ID column '{col}' values were changed. Restoring original ID values...")
                            modified_df[col] = original_df[col]
                            
                    except (ValueError, TypeError):
                        st.error(f"❌ Column '{col}' contains non-integer values. The AI may have mixed up column order.")
                        st.error(f"First value in {col}: {first_val}")
                        st.error("Please try the modification again with clearer instructions.")
                        return None
            
            # Ensure we have the right number of rows
            if len(modified_df) != len(original_df):
                st.warning(f"⚠️ Row count mismatch. Expected {len(original_df)}, got {len(modified_df)}. Adjusting...")
                if len(modified_df) < len(original_df):
                    # Pad with original data
                    missing_rows = len(original_df) - len(modified_df)
                    padding_df = original_df.tail(missing_rows).copy()
                    modified_df = pd.concat([modified_df, padding_df], ignore_index=True)
                else:
                    # Truncate to match original
                    modified_df = modified_df.head(len(original_df))
            
            # Ensure data types match with better error handling
            for col in original_df.columns:
                if col in modified_df.columns:
                    try:
                        # Special handling for different data types
                        if original_df[col].dtype == 'int64':
                            # Try to convert to int, handle non-numeric values
                            modified_df[col] = pd.to_numeric(modified_df[col], errors='coerce').fillna(original_df[col]).astype('int64')
                        elif original_df[col].dtype == 'float64':
                            modified_df[col] = pd.to_numeric(modified_df[col], errors='coerce').fillna(original_df[col]).astype('float64')
                        elif original_df[col].dtype == 'bool':
                            # Handle boolean conversion
                            modified_df[col] = modified_df[col].astype(str).str.lower().map({'true': True, 'false': False}).fillna(original_df[col])
                        else:
                            # For string and other types, try direct conversion
                            modified_df[col] = modified_df[col].astype(original_df[col].dtype)
                    except Exception as type_error:
                        st.warning(f"⚠️ Could not convert column '{col}' to {original_df[col].dtype}. Using original data.")
                        modified_df[col] = original_df[col]
            
            st.success(f"✅ Successfully modified {table_name} with AI!")
            return modified_df
            
        except Exception as parse_error:
            st.error(f"❌ Failed to parse AI response as CSV: {str(parse_error)}")
            st.error("AI Response:")
            st.code(response)
            
            # Show debugging information
            with st.expander("🔍 Debug Information"):
                st.write("**Original DataFrame Info:**")
                st.write(f"Columns: {list(original_df.columns)}")
                st.write(f"Data Types: {original_df.dtypes.to_dict()}")
                st.write(f"Shape: {original_df.shape}")
                
                st.write("**AI Response (first 500 chars):**")
                st.code(response[:500] + "..." if len(response) > 500 else response)
            
            return None
        
    except Exception as e:
        st.error(f"Failed to modify table {table_name}: {str(e)}")
        return None

def process_data_modification(prompt):
    """Process data modification request using AI"""
    try:
        # Validate input
        if not prompt or prompt.strip() == "":
            return "Please provide specific instructions for data modification."
        
        # Check if we have generated data
        if "generated_tables" not in st.session_state:
            return "No generated data available for modification. Please generate data first."
        
        # For now, provide guidance on using the table-specific modification interface
        table_names = list(st.session_state["generated_tables"].keys())
        response = f"I understand you want to modify the data: '{prompt}'. "
        response += f"Currently, you have {len(table_names)} tables available: {', '.join(table_names)}. "
        response += "For specific modifications, please use the individual table modification interface above each table. "
        response += "You can describe changes like 'make the data more realistic', 'add more variety', or 'change the age distribution'."
        
        return response
        
    except Exception as e:
        return f"Error processing modification request: {str(e)}"

def generate_data_workflow_from_ddl(instructions, temperature, num_records, ddl_content, drop_existing=False):
    """Main data generation workflow from DDL content"""
    start_time = time.time()
    
    try:
        observability.log_user_action("data_generation_workflow_from_ddl_start",
                                    instructions_provided=bool(instructions),
                                    temperature=temperature,
                                    num_records=num_records,
                                    ddl_length=len(ddl_content),
                                    drop_existing=drop_existing)
        
        # Validate inputs
        if not ddl_content or ddl_content.strip() == "":
            observability.log_user_action("validation_failed", reason="no_ddl_content")
            st.error("No DDL content provided. Please upload a schema file first.")
            return
        
        if not instructions or instructions.strip() == "":
            instructions = "Generate realistic, diverse data that follows common patterns and constraints"
            observability.log_info("Using default instructions for data generation")
        
        with st.spinner("Generating AI data from PostgreSQL schema..."):
            observability.log_workflow_step("main_workflow_ddl", "ai_generation", "start")
            # Check authentication before proceeding
            auth_status = auth_manager.get_authentication_status()
            if not auth_status["authenticated"]:
                observability.log_workflow_step("main_workflow_ddl", "ai_generation", "error",
                                              error="authentication_required")
                st.error("❌ Authentication required for data generation. Please authenticate first.")
                return
            
            data_generator = DataGenerationOrchestrator()
            generated_data = data_generator.generate_from_ddl(
                ddl_content=ddl_content,
                instructions=instructions,
                temperature=temperature,
                num_records=num_records
            )
        
        if generated_data:
            observability.log_workflow_step("main_workflow_ddl", "ai_generation", "success",
                                          tables_generated=len(generated_data))
            
            # Create tables in PostgreSQL and store data
            with st.spinner("Creating tables in PostgreSQL and storing data..."):
                from core.database_manager import DatabaseManager
                
                db_manager = DatabaseManager()
                # Create tables from DDL with drop_existing option
                db_manager.create_tables_from_ddl(ddl_content, drop_existing=drop_existing)
                
                # Store generated data in PostgreSQL with proper dependency order
                db_manager.store_generated_data(generated_data, ddl_content)
            
            # Store in session state for compatibility (but data is now in PostgreSQL)
            st.session_state["generated_tables"] = generated_data
            
            # Store schema info for query generation
            from core.ddl_parser import DDLParser
            ddl_parser = DDLParser()
            parsed_tables = ddl_parser.parse_ddl(ddl_content)
            # Convert to schema info format for query generation
            schema_info = {}
            for table in parsed_tables:
                schema_info[table.name] = {
                    'columns': [{'name': col.name, 'type': col.data_type.value, 'nullable': col.nullable} for col in table.columns],
                    'primary_key': table.primary_keys,
                    'foreign_keys': table.foreign_keys
                }
            st.session_state["schema_info"] = schema_info
            
            # Verify schema match for debugging
            try:
                db_manager = DatabaseManager()
                if db_manager.is_connected():
                    verification_results = db_manager.verify_schema_match(schema_info)
                    # Log any mismatches
                    for table_name, result in verification_results.items():
                        if result['status'] == 'mismatch':
                            observability.log_info(f"Schema mismatch for {table_name}: missing={result['missing_columns']}, extra={result['extra_columns']}")
                        elif result['status'] == 'missing':
                            observability.log_info(f"Table {table_name} missing from database: {result['message']}")
            except Exception as e:
                observability.log_info(f"Schema verification failed: {e}")
            
            total_duration = time.time() - start_time
            observability.log_user_action("data_generation_workflow_from_ddl_success",
                                        total_duration=total_duration,
                                        tables_generated=len(generated_data))
            observability.log_performance("complete_data_generation_workflow_from_ddl", total_duration,
                                        tables=len(generated_data),
                                        records_per_table=num_records)
            
            st.success("✅ Data generation completed successfully! Data is now stored in PostgreSQL.")
            st.rerun()
        else:
            observability.log_workflow_step("main_workflow_ddl", "ai_generation", "error",
                                          error="no_data_generated")
            st.error("❌ Data generation failed. No data was generated for any table.")
            
            # Enhanced troubleshooting section
            with st.expander("🔧 Troubleshooting Guide", expanded=True):
                st.markdown("### Common Issues and Solutions:")
                
                col1, col2 = st.columns(2)
                
                with col1:
                    st.markdown("**🔐 Authentication Issues:**")
                    st.markdown("• Check your Gemini API key in the sidebar")
                    st.markdown("• Ensure the API key is valid and has proper permissions")
                    st.markdown("• Try re-authenticating by clicking 'Authenticate' again")
                    
                    st.markdown("**🌐 Network Issues:**")
                    st.markdown("• Check your internet connection")
                    st.markdown("• Verify you can access Google's services")
                    st.markdown("• Try again in a few minutes")
                
                with col2:
                    st.markdown("**📊 Schema Issues:**")
                    st.markdown("• Verify your DDL file contains valid PostgreSQL CREATE TABLE statements")
                    st.markdown("• Check for unsupported data types")
                    st.markdown("• Ensure table names don't contain special characters")
                    
                    st.markdown("**🤖 AI Service Issues:**")
                    st.markdown("• Gemini API might be experiencing high load")
                    st.markdown("• Check Google AI Studio status")
                    st.markdown("• Try reducing the number of records per table")
                
                st.markdown("### Debug Information:")
                auth_status = auth_manager.get_authentication_status()
                st.json({
                    "Authentication Status": auth_status["authenticated"],
                    "Authentication Method": auth_status.get("method", "None"),
                    "Tables in Schema": len(st.session_state.get("generated_tables", {})),
                    "Session State Keys": list(st.session_state.keys())
                })
            
    except Exception as e:
        total_duration = time.time() - start_time
        observability.log_user_action("data_generation_workflow_from_ddl_error",
                                    error=str(e),
                                    duration=total_duration)
        observability.log_exception(e, "data_generation_workflow_from_ddl")
        st.error(f"Data generation failed: {str(e)}")
        # Log the error for debugging
        import traceback
        st.error(f"Error details: {traceback.format_exc()}")

# Page title and description
st.title("Data Generation")
st.caption("Generate synthetic data with AI-powered customization")

# Sidebar authentication
with st.sidebar:
    st.subheader("🔐 Authentication")
    
    # Use the minimal authentication UI
    auth_manager.get_authentication_ui()
    


# Schema input section
st.subheader("📁 Upload PostgreSQL Schema")
st.info("📌 **Upload a PostgreSQL DDL schema file (.sql, .txt, or .ddl) and the AI will generate realistic data for all tables.**")

uploaded_file = st.file_uploader(
    "Upload PostgreSQL DDL Schema File", 
    type=["sql", "txt", "ddl"],
    help="Upload your PostgreSQL database schema file (.sql, .txt, or .ddl). The system will parse all CREATE TABLE statements and generate data for each table."
)

if uploaded_file:
    try:
        observability.log_user_action("file_upload_attempt",
                                    filename=uploaded_file.name,
                                    file_size=uploaded_file.size,
                                    file_type=uploaded_file.type)
        
        # Validate file size (max 10MB)
        if uploaded_file.size > 10 * 1024 * 1024:
            observability.log_user_action("file_upload_rejected", 
                                        reason="file_too_large",
                                        file_size=uploaded_file.size)
            st.error("File too large. Please upload a file smaller than 10MB.")
        else:
            # Read and validate file content
            file_content = uploaded_file.read().decode('utf-8')
            
            # Basic validation - check if it contains SQL keywords
            sql_keywords = ['CREATE', 'TABLE', 'INSERT', 'ALTER', 'DROP']
            has_sql_keywords = any(keyword in file_content.upper() for keyword in sql_keywords)
            
            if not has_sql_keywords:
                observability.log_user_action("file_upload_warning",
                                            reason="no_sql_keywords",
                                            filename=uploaded_file.name)
                st.warning("⚠️ File doesn't appear to contain SQL statements. Please check your file.")
            else:
                observability.log_user_action("file_upload_success",
                                            filename=uploaded_file.name,
                                            file_size=uploaded_file.size,
                                            content_length=len(file_content))
                st.success(f"✅ Uploaded: {uploaded_file.name} ({uploaded_file.size} bytes)")
                # Store in session state
                st.session_state["uploaded_ddl"] = file_content
                
                # Show preview of DDL content
                with st.expander("📋 Preview DDL Content"):
                    st.code(file_content[:1000] + "..." if len(file_content) > 1000 else file_content, language="sql")
                    
    except UnicodeDecodeError:
        observability.log_user_action("file_upload_error",
                                    error="unicode_decode_error",
                                    filename=uploaded_file.name)
        st.error("❌ File encoding error. Please ensure your file is UTF-8 encoded.")
    except Exception as e:
        observability.log_user_action("file_upload_error",
                                    error=str(e),
                                    filename=uploaded_file.name)
        observability.log_exception(e, "file_upload")
        st.error(f"❌ Error reading file: {str(e)}")


# User input interface
if uploaded_file:
    st.subheader("📝 Data Generation Instructions (Optional)")
    instructions = st.text_area(
        "Describe how you want the data to be generated (optional):",
        placeholder="e.g., 'Generate realistic user data for a tech startup with diverse backgrounds' or 'Create sales data for the last 6 months with seasonal patterns'. Leave empty to generate standard realistic data.",
        height=100,
        key="data_instructions",
        help="You can generate data without instructions - the system will create realistic data based on your schema"
    )
    
    # Generation parameters
    st.subheader("⚙️ Generation Parameters")
    
    col1, col2 = st.columns(2)
    
    with col1:
        temperature = st.slider(
            "Temperature (Creativity)",
            min_value=0.0,
            max_value=2.0,
            value=settings.DEFAULT_TEMPERATURE,
            step=0.05,
            help="Lower values = more consistent data (recommended: 0.05-0.2 for structured data)"
        )
    
    with col2:
        num_records = st.number_input(
            "Records per Table",
            min_value=1,
            max_value=10000,
            value=settings.DEFAULT_RECORDS_PER_TABLE,
            step=1,
            help="Number of records to generate for each table"
        )
    
    
    # Generate button
    if st.button("Generate Data", type="primary"):
        observability.log_user_action("generate_data_button_clicked",
                                    temperature=temperature,
                                    num_records=num_records,
                                    has_instructions=bool(instructions),
                                    drop_existing=True)
        
        auth_status = auth_manager.get_authentication_status()
        if not auth_status["authenticated"] and "gemini_client" not in st.session_state:
            observability.log_user_action("generate_data_blocked", reason="not_authenticated")
            st.error("Please authenticate to continue.")
        else:
            # Trigger data generation process
            if uploaded_file and "uploaded_ddl" in st.session_state:
                # Use DDL file
                generate_data_workflow_from_ddl(instructions, temperature, num_records, st.session_state["uploaded_ddl"], drop_existing=True)

# Data preview interface
if "generated_tables" in st.session_state:
    st.subheader("📊 Generated Data Preview")
    
    # Create tabs for each table
    table_names = list(st.session_state["generated_tables"].keys())
    tabs = st.tabs(table_names)
    
    for i, (table_name, tab) in enumerate(zip(table_names, tabs)):
        with tab:
            # Display table data
            df = st.session_state["generated_tables"][table_name]
            st.dataframe(df, width='stretch', hide_index=True)
            
            # Table statistics
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Records", len(df))
            with col2:
                st.metric("Columns", len(df.columns))
            with col3:
                st.metric("Memory", f"{df.memory_usage(deep=True).sum() / 1024:.1f} KB")
            
            # Edit interface for each table
            st.subheader(f"✏️ Modify {table_name}")
            
            # Create tabs for different modification methods
            mod_tab1, mod_tab2 = st.tabs(["🤖 AI-Powered Modification", "✏️ Interactive Editor"])
            
            with mod_tab1:
                st.write("**AI-Powered Data Modification**")
                st.write("Describe the changes you want to make and let AI modify the data:")
                
                edit_prompt = st.text_area(
                    f"Describe changes for {table_name}:",
                    placeholder=f"e.g., 'Make all {table_name} records more realistic' or 'Add more variety to {table_name}'",
                    key=f"edit_{table_name}"
                )
                
                if st.button(f"Apply AI Changes to {table_name}", key=f"apply_{table_name}"):
                    if edit_prompt:
                        # Process edit request
                        modified_data = process_table_edit(table_name, edit_prompt)
                        if modified_data is not None:
                            st.session_state["generated_tables"][table_name] = modified_data
                            st.success(f"✅ Updated {table_name} with AI modifications")
                            st.rerun()
                    else:
                        st.warning("Please provide modification instructions.")
            
            with mod_tab2:
                st.write("**Interactive Data Editor**")
                st.write("Edit the data directly in the table below:")
                
                # Use st.data_editor for interactive editing
                edited_df = st.data_editor(
                    df,
                    key=f"editor_{table_name}",
                    num_rows="dynamic",
                    width='stretch',
                    hide_index=True
                )
                
                # Check if data was modified
                if not edited_df.equals(df):
                    col1, col2 = st.columns([1, 1])
                    with col1:
                        if st.button(f"💾 Save Changes to {table_name}", key=f"save_{table_name}"):
                            st.session_state["generated_tables"][table_name] = edited_df
                            st.success(f"✅ Saved manual edits to {table_name}")
                            st.rerun()
                    
                    with col2:
                        if st.button(f"🔄 Reset {table_name}", key=f"reset_{table_name}"):
                            st.rerun()

# Export functionality
if "generated_tables" in st.session_state:
    st.subheader("📥 Download Generated Data")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Direct CSV Download
        try:
            csv_data = export_manager.create_csv_export(st.session_state["generated_tables"])
            st.download_button(
                label="📄 Download CSV",
                data=csv_data,
                file_name="generated_data.csv",
                mime="text/csv",
                type="primary"
            )
        except Exception as e:
            st.error(f"CSV export failed: {str(e)}")
    
    with col2:
        # Direct ZIP Download
        try:
            zip_data = export_manager.create_zip_export(st.session_state["generated_tables"])
            st.download_button(
                label="📦 Download ZIP",
                data=zip_data,
                file_name="generated_data.zip",
                mime="application/zip",
                type="primary"
            )
        except Exception as e:
            st.error(f"ZIP export failed: {str(e)}")