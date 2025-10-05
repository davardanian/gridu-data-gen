import streamlit as st
import pandas as pd
from datetime import datetime
from core.auth_manager import auth_manager
from core.query_generator import QueryGenerator
from core.guardrails import GuardrailsManager
from utils.visualization import VisualizationManager
from core.observability import observability

# Initialize managers
query_generator = QueryGenerator()
guardrails = GuardrailsManager()
viz_manager = VisualizationManager()

# Helper functions
def process_data_query(prompt):
    """Process user query and generate appropriate response"""
    # Log user interaction
    observability.log_user_action(
        "data_query_submitted",
        query_length=len(prompt),
        has_data=bool(st.session_state.get("generated_tables")),
        tables_available=len(st.session_state.get("generated_tables", {}))
    )
    
    with st.chat_message("assistant"):
        # Determine query type
        query_type = query_generator.classify_query_type(prompt)
        
        # Log query classification
        observability.log_user_action(
            "query_classified",
            query_type=query_type,
            original_query=prompt[:100] + "..." if len(prompt) > 100 else prompt
        )
        
        if query_type == "sql_generation":
            response = handle_sql_generation(prompt)
        elif query_type == "data_analysis":
            response = handle_data_analysis(prompt)
        elif query_type == "visualization":
            response = handle_visualization_request(prompt)
        else:
            response = handle_general_query(prompt)
        
        # Log response generation
        observability.log_user_action(
            "query_response_generated",
            query_type=query_type,
            response_length=len(response) if response else 0,
            success=bool(response and response != "Error")
        )
        
        # Display response 
        st.chat_message("assistant").write(response)
        
        # Add to message history
        st.session_state.messages.append({
            "role": "assistant", 
            "content": response,
            "query_type": query_type
        })

def handle_sql_generation(prompt):
    """Generate and execute SQL queries with AI response"""
    try:
        # Get schema info
        schema_info = st.session_state.get("schema_info", {})
        
        # Generate SQL query using AI
        sql_query = query_generator.generate_sql_query(prompt, schema_info)
        
        if sql_query:
            # Display SQL query in a code block
            st.code(sql_query, language="sql")
            
            # Execute the SQL query against PostgreSQL
            with st.spinner("Executing query..."):
                results = query_generator.execute_query(sql_query)
            
            if results is not None and not results.empty:
                # Display results
                st.dataframe(results, width='stretch')
                
                # Add to query history
                st.session_state.query_history.append({
                    "prompt": prompt,
                    "sql": sql_query,
                    "results": results,
                    "timestamp": datetime.now()
                })
                
                # Generate concise AI response
                context = {
                    "generated_tables": st.session_state.get("generated_tables", {}),
                    "schema_info": schema_info,
                    "sql_query": sql_query,
                    "query_results": results
                }
                
                return f"✅ Found {len(results)} results"
            else:
                return "No results found"
                
        else:
            # If query generation fails, provide helpful guidance
            context = {
                "generated_tables": st.session_state.get("generated_tables", {}),
                "schema_info": schema_info
            }
            return "❌ Could not generate query"
            
    except Exception as e:
        st.error(f"Error executing query: {str(e)}")
        context = {
            "generated_tables": st.session_state.get("generated_tables", {}),
            "schema_info": st.session_state.get("schema_info", {})
        }
        return f"❌ Error: {str(e)}"

def handle_visualization_request(prompt):
    """Generate data visualizations with AI response using database queries"""
    try:
        # Get schema info
        schema_info = st.session_state.get("schema_info", {})
        
        # Generate SQL query for visualization using AI
        viz_sql_query = query_generator.generate_visualization_query(prompt, schema_info)
        
        if viz_sql_query:
            # Display SQL query in a code block
            st.code(viz_sql_query, language="sql")
            
            # Execute the SQL query against PostgreSQL
            with st.spinner("Executing visualization query..."):
                results = query_generator.execute_query(viz_sql_query)
            
            if results is not None and not results.empty:
                # Display results
                st.dataframe(results, width='stretch')
                
                # Create visualization from query results
                with st.spinner("Creating visualization..."):
                    chart = query_generator.create_visualization_from_query_results(results, prompt)
                
                if chart:
                    st.pyplot(chart)
                    
                    # Add to query history
                    st.session_state.query_history.append({
                        "prompt": prompt,
                        "sql": viz_sql_query,
                        "results": results,
                        "timestamp": datetime.now(),
                        "visualization": True
                    })
                    
                    # Generate concise AI response
                    context = {
                        "generated_tables": st.session_state.get("generated_tables", {}),
                        "schema_info": schema_info,
                        "sql_query": viz_sql_query,
                        "query_results": results,
                        "visualization_created": True
                    }
                    
                    return "✅ Chart created"
                else:
                    return "❌ Could not create chart"
                    
            else:
                return "No data found"
                
        else:
            # If visualization query generation fails, provide helpful guidance
            return "❌ Could not create chart"
            
    except Exception as e:
        st.error(f"Error creating visualization: {str(e)}")
        return f"❌ Error: {str(e)}"

def handle_data_analysis(prompt):
    """Handle data analysis requests with AI response"""
    try:
        # Generate data summary
        summary = generate_data_summary(prompt)
        
        # Create context for AI response
        context = {
            "generated_tables": st.session_state.get("generated_tables", {}),
            "schema_info": st.session_state.get("schema_info", {}),
            "data_summary": summary
        }
        
        # Generate minimal AI response about the analysis
        return "📊 Analysis done"
        
    except Exception as e:
        context = {
            "generated_tables": st.session_state.get("generated_tables", {}),
            "schema_info": st.session_state.get("schema_info", {})
        }
        return f"❌ Error: {str(e)}"

def handle_general_query(prompt):
    """Handle general queries about the data with AI response"""
    try:
        # Get data summary
        summary = generate_data_summary(prompt)
        
        # Create context for AI response
        context = {
            "generated_tables": st.session_state.get("generated_tables", {}),
            "schema_info": st.session_state.get("schema_info", {}),
            "data_summary": summary
        }
        
        # Generate minimal AI response about the data
        return "💬 Data available"
        
    except Exception as e:
        context = {
            "generated_tables": st.session_state.get("generated_tables", {}),
            "schema_info": st.session_state.get("schema_info", {})
        }
        return f"❌ Error: {str(e)}"


def generate_data_summary(prompt):
    """Generate statistical summary of data"""
    try:
        # Generate summary for all tables
        summaries = []
        
        for table_name, df in st.session_state["generated_tables"].items():
            summary = viz_manager.get_data_summary(df)
            summaries.append(f"**{table_name}:**\n{summary}")
        
        return "\n\n".join(summaries)
        
    except Exception as e:
        return f"Error generating summary: {str(e)}"

# Page title and description
st.title("💬 Talk to Your Data")
st.caption("Interactive data analysis powered by Google Gemini")

# Help section with examples
with st.expander("💡 How to Ask Questions - Get the Best Results", expanded=False):
    st.markdown("""
    ### 🎯 **Query Types & Examples**
    
    The AI understands different types of requests. Here's how to ask for what you want:
    
    #### 📊 **SQL Queries** (Data Retrieval)
    - **"Show me all users with age greater than 30"**
    - **"Find the average salary by department"**
    - **"List the top 10 customers by purchase amount"**
    - **"Count how many orders were placed last month"**
    - **"Get all products with price above $100"**
    
    #### 📈 **Visualizations** (Charts & Graphs)
    - **"Create a bar chart of sales by region"**
    - **"Show me a line chart of revenue over time"**
    - **"Make a scatter plot of age vs salary"**
    - **"Generate a histogram of product prices"**
    - **"Create a heatmap of customer satisfaction by department"**
    
    #### 🔍 **Data Analysis** (Insights & Summaries)
    - **"Analyze the customer data and give me insights"**
    - **"What patterns do you see in the sales data?"**
    - **"Summarize the key statistics of our products"**
    - **"Describe the distribution of employee salaries"**
    
    #### 💬 **General Questions** (Data Understanding)
    - **"What data do I have available?"**
    - **"Explain what this table contains"**
    - **"What can I do with this dataset?"**
    - **"Help me understand my data better"**
    """)

# Check if data is available
if "generated_tables" not in st.session_state:
    st.error("No generated data available. Please generate data first in the Data Generation page.")
    st.info("Go to the Data Generation page to upload a DDL schema and generate synthetic data.")
    st.stop()

# Sidebar authentication and data overview
with st.sidebar:
    st.subheader("🔐 Authentication")
    
    # Use the minimal authentication UI
    auth_manager.get_authentication_ui()
    
  
    
    # Data status indicator
    st.subheader("📊 Data Overview")
    st.success("✅ Data Available")
    st.info(f"Tables: {len(st.session_state['generated_tables'])}")
    
    # Database connection status
    st.subheader("🗄️ Database Status")
    from core.database_manager import DatabaseManager
    
    db_manager = DatabaseManager()
    if db_manager.is_connected():
        st.success("✅ PostgreSQL Connected")
        st.caption("SQL queries are executed against the PostgreSQL database.")
    else:
        st.error("❌ Database Disconnected")
        st.caption("Please ensure PostgreSQL is running and properly configured.")
    

    
    # Show table information
    for table_name, df in st.session_state["generated_tables"].items():
        with st.expander(f"📋 {table_name}"):
            st.metric("Records", len(df))
            st.metric("Columns", len(df.columns))
            
            # Show column types
            st.text("Column Types:")
            for col, dtype in df.dtypes.items():
                st.text(f"  {col}: {dtype}")
            
            # Add helpful query suggestions for this table
            st.markdown("**💡 Try asking:**")
            numerical_cols = df.select_dtypes(include=['int64', 'float64']).columns
            categorical_cols = df.select_dtypes(include=['object', 'category']).columns
            
            if len(numerical_cols) > 0 and len(categorical_cols) > 0:
                st.text(f"• 'Show me {numerical_cols[0]} by {categorical_cols[0]}'")
                st.text(f"• 'Create a bar chart of {numerical_cols[0]} by {categorical_cols[0]}'")
            elif len(numerical_cols) > 0:
                st.text(f"• 'Show me the average {numerical_cols[0]}'")
                st.text(f"• 'Create a histogram of {numerical_cols[0]}'")
            elif len(categorical_cols) > 0:
                st.text(f"• 'Count records by {categorical_cols[0]}'")
                st.text(f"• 'Show me the distribution of {categorical_cols[0]}'")

# Initialize chat messages with data context
if "messages" not in st.session_state:
    table_names = list(st.session_state['generated_tables'].keys())
    st.session_state.messages = [
        {
            "role": "assistant", 
            "content": f"""👋 **Welcome to your data analysis assistant!** 

I can help you analyze your data in multiple ways:

📊 **SQL Queries**: "Show me all users with age > 30"
📈 **Visualizations**: "Create a bar chart of sales by region"  
🔍 **Data Analysis**: "Analyze the customer data and give me insights"
💬 **General Questions**: "What data do I have available?"

You have **{len(st.session_state['generated_tables'])} tables** available: **{', '.join(table_names)}**

💡 **Tip**: Check the "How to Ask Questions" section above for detailed examples and best practices!

What would you like to explore first?"""
        }
    ]

# Initialize query history
if "query_history" not in st.session_state:
    st.session_state.query_history = []

# Display chat messages with enhanced formatting
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])
        
        # Display SQL queries if present
        if message.get("sql_query"):
            st.code(message["sql_query"], language="sql")
        
        # Display query results if present
        if message.get("query_results") is not None:
            st.dataframe(message["query_results"], width='stretch')
        
        # Display visualizations if present
        if message.get("visualization"):
            st.pyplot(message["visualization"])

# Enhanced chat input
if prompt := st.chat_input("💬 Ask me about your data... (e.g., 'Show me sales by region', 'Create a bar chart', 'Analyze the data')"):
    # Log user input attempt
    observability.log_user_action(
        "chat_input_attempt",
        input_length=len(prompt),
        session_messages=len(st.session_state.get("messages", []))
    )
    
    auth_status = auth_manager.get_authentication_status()
    if not auth_status["authenticated"] and "gemini_client" not in st.session_state:
        observability.log_user_action(
            "chat_input_blocked",
            reason="not_authenticated"
        )
        st.error("Please authenticate to continue.")
        st.stop()
    
    # Validate input
    validation = guardrails.validate_input(prompt, "query")
    if not validation["is_valid"]:
        observability.log_user_action(
            "chat_input_blocked",
            reason="validation_failed",
            validation_errors=validation.get("errors", [])
        )
        st.error("Invalid input detected. Please try again.")
        st.stop()
    
    # Log successful input validation
    observability.log_user_action(
        "chat_input_validated",
        input_length=len(prompt)
    )
    
    # Add user message
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)
    
    # Process the query
    process_data_query(prompt)

# Query history section
if st.session_state.query_history:
    with st.expander("📊 Query History"):
        for i, query in enumerate(reversed(st.session_state.query_history[-10:])):  # Show last 10
            with st.container():
                query_type = "Visualization" if query.get("visualization") else "SQL Query"
                st.text(f"{query_type} {len(st.session_state.query_history) - i}: {query['prompt']}")
                st.code(query['sql'], language="sql")
                
                col1, col2 = st.columns([3, 1])
                with col1:
                    st.dataframe(query['results'], width='stretch')
                with col2:
                    if st.button("Re-run", key=f"rerun_{i}"):
                        # Re-execute query
                        st.session_state.messages.append({
                            "role": "user", 
                            "content": f"Re-run: {query['prompt']}"
                        })
                        st.rerun()
