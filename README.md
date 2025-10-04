# Data Assistant Application

A powerful Streamlit application that provides AI-powered synthetic data generation and interactive data analysis capabilities.

## Features

### 🎲 Data Generation
- Upload DDL schema files (.sql, .txt, .ddl)
- AI-powered synthetic data generation with customizable instructions
- Real-time data preview with table-by-table display
- Interactive data modification through natural language
- Export data as CSV or ZIP files

### 💬 Talk to Your Data
- Natural language to SQL query generation
- Interactive chat interface for data analysis
- Data visualization with Seaborn charts
- Query history and re-execution
- Schema-aware responses

## Architecture

- **Frontend**: Streamlit with native components only
- **Database**: PostgreSQL with `st.connection` integration
- **AI**: Google Gemini for natural language processing
- **Visualization**: Seaborn/Matplotlib charts via `st.pyplot()`
- **Security**: Comprehensive guardrails and PII protection
- **Observability**: Langfuse integration for tracing

## Prerequisites

### Option 1: Google Cloud CLI Authentication (Recommended)
```bash
gcloud auth application-default login
gcloud config set project YOUR_PROJECT_ID
```

### Option 2: Gemini API Key (Fallback)
1. Get a Gemini API key from [Google AI Studio](https://makersuite.google.com/app/apikey)
2. Set the `GEMINI_API_KEY` environment variable

## Quick Start

### Using Docker (Recommended)

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd gridu-data-gen
   ```

2. Create environment file:
   ```bash
   # Create .env file with your configuration
   # See Configuration section below for required variables
   ```

3. Start the application:
   ```bash
   docker-compose up --build
   ```

4. Open your browser to `http://localhost:8501`



## Configuration

### Environment Variables

Copy `.env.example` to `.env` and configure your settings:

```bash
cp .env.example .env
```

Edit `.env` with your actual values (API keys, project ID, etc.).

### AI Configuration Options

The application provides fine-grained control over AI token usage through environment variables:

- **`MAX_OUTPUT_TOKENS`**: Default maximum tokens for general AI operations (default: 65535)
- **`MAX_DATA_GENERATION_TOKENS`**: Maximum tokens for data generation tasks (default: 65535)
- **`MAX_QUERY_GENERATION_TOKENS`**: Maximum tokens for SQL query generation and conversational AI (default: 1000)
- **`GEMINI_MODEL`**: Gemini model to use (default: gemini-2.5-flash)
- **`DEFAULT_QUERY_TEMPERATURE`**: Temperature setting for query generation (default: 0.1)

These settings allow you to optimize token usage based on your specific needs and budget constraints.

**Note**: Database configuration is handled through environment variables in the `.env` file. The application uses SQLAlchemy with the `DATABASE_URL` environment variable for all database connections.

## Usage

### 1. Data Generation (Main Page)

1. Start the application - you'll land on the "Data Generation" page
2. Upload a DDL schema file (.sql, .txt, or .ddl)
3. Optionally provide custom instructions for data generation
4. Adjust generation parameters (temperature, number of records)
5. Click "Generate Data"
6. Preview and modify generated data
7. Export data as CSV or ZIP

### 2. Talk to Your Data

1. Navigate to the "Talk to Your Data" page from the sidebar
2. Ask questions about your data in natural language
3. Request SQL queries, visualizations, or data analysis
4. View query history and re-run previous queries
5. Explore data through interactive charts





## Security Features

- **Prompt Injection Detection**: Prevents malicious prompt manipulation
- **PII Protection**: Automatically detects and sanitizes personal information
- **SQL Injection Prevention**: Validates and sanitizes SQL queries
- **Input Validation**: Comprehensive input validation and sanitization
- **Audit Logging**: Security event tracking and monitoring
