# config/settings.py
import os
from dotenv import load_dotenv
from typing import Optional

# Load environment variables from .env file
# Try multiple paths to find the .env file
env_paths = ['.env', '../.env', '../../.env']
for env_path in env_paths:
    if os.path.exists(env_path):
        load_dotenv(env_path)
        break
else:
    # If no .env file found, try loading from current directory
    load_dotenv()

class Settings:
    """Application settings and configuration"""
    
    # Gemini AI Configuration
    GEMINI_API_KEY: Optional[str] = os.getenv("GEMINI_API_KEY")
    
    # Vertex AI Configuration (uses Google Application Default Credentials)
    PROJECT_ID: Optional[str] = os.getenv("PROJECT_ID")
    LOCATION: str = os.getenv("LOCATION", "us-central1")
    
    # Set GOOGLE_CLOUD_PROJECT for Google Auth library compatibility
    if PROJECT_ID and not os.getenv("GOOGLE_CLOUD_PROJECT"):
        os.environ["GOOGLE_CLOUD_PROJECT"] = PROJECT_ID
    
    # Database Configuration
    DB_HOST: str = os.getenv("DB_HOST", "localhost")
    DB_PORT: int = int(os.getenv("DB_PORT", "5432"))
    DB_NAME: str = os.getenv("DB_NAME", "data_assistant")
    DB_USER: str = os.getenv("DB_USER", "postgres")
    DB_PASSWORD: str = os.getenv("DB_PASSWORD", "password")
    
    # Construct DATABASE_URL from individual components
    DATABASE_URL: str = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    
    # Langfuse Configuration
    LANGFUSE_PUBLIC_KEY: Optional[str] = os.getenv("LANGFUSE_PUBLIC_KEY")
    LANGFUSE_SECRET_KEY: Optional[str] = os.getenv("LANGFUSE_SECRET_KEY")
    LANGFUSE_HOST: str = os.getenv("LANGFUSE_HOST", "https://cloud.langfuse.com")
    
    # Application Configuration
    APP_ENV: str = os.getenv("APP_ENV", "development")
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "DEBUG")
    DEBUG: bool = os.getenv("DEBUG", "true").lower() == "true"
    
    # Data Generation Configuration
    DEFAULT_TEMPERATURE: float = float(os.getenv("DEFAULT_TEMPERATURE", "0.4"))
    DEFAULT_RECORDS_PER_TABLE: int = int(os.getenv("DEFAULT_RECORDS_PER_TABLE", "100"))
    MAX_RECORDS_PER_TABLE: int = int(os.getenv("MAX_RECORDS_PER_TABLE", "10000"))
    FAST_MODE_RECORDS: int = int(os.getenv("FAST_MODE_RECORDS", "20"))  # For quick testing
    DEFAULT_INSTRUCTIONS: str = os.getenv("DEFAULT_INSTRUCTIONS", "Generate realistic, diverse data that follows common patterns and constraints")
    
    # AI Model Configuration
    GEMINI_MODEL: str = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
    MAX_OUTPUT_TOKENS: int = int(os.getenv("MAX_OUTPUT_TOKENS", "65535"))
    MAX_DATA_GENERATION_TOKENS: int = int(os.getenv("MAX_DATA_GENERATION_TOKENS", "65535"))
    MAX_QUERY_GENERATION_TOKENS: int = int(os.getenv("MAX_QUERY_GENERATION_TOKENS", "1000"))
    DEFAULT_QUERY_TEMPERATURE: float = float(os.getenv("DEFAULT_QUERY_TEMPERATURE", "0.1"))

# Global settings instance
settings = Settings()

