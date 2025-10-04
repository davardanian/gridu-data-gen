# core/observability.py
import logging
import sys
import json
from datetime import datetime
from typing import Optional, Dict, Any
from langfuse import Langfuse
from config.settings import settings

class ObservabilityManager:
    """Manages logging and observability with Langfuse and comprehensive Docker logging"""
    
    def __init__(self):
        self.langfuse = None
        self.logger = self._setup_logging()
        self._initialize_langfuse()
        self._log_startup()
    
    def _setup_logging(self) -> logging.Logger:
        """Setup comprehensive application logging for Docker"""
        # Create main logger
        logger = logging.getLogger("data_assistant")
        logger.setLevel(getattr(logging, settings.LOG_LEVEL))
        
        # Clear any existing handlers to avoid duplicates
        logger.handlers.clear()
        
        # Create structured formatter for Docker logs
        formatter = logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(name)-20s | %(funcName)-15s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Create console handler (stdout for Docker)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        console_handler.setLevel(getattr(logging, settings.LOG_LEVEL))
        logger.addHandler(console_handler)
        
        # Create error handler (stderr for Docker)
        error_handler = logging.StreamHandler(sys.stderr)
        error_handler.setFormatter(formatter)
        error_handler.setLevel(logging.ERROR)
        logger.addHandler(error_handler)
        
        # Prevent propagation to avoid duplicate logs
        logger.propagate = False
        
        return logger
    
    def _log_startup(self):
        """Log application startup information"""
        self.logger.info("=" * 80)
        self.logger.info("🚀 DATA GENERATION APPLICATION STARTING")
        self.logger.info("=" * 80)
        self.logger.info(f"Environment: {settings.APP_ENV}")
        self.logger.info(f"Log Level: {settings.LOG_LEVEL}")
        self.logger.info(f"Debug Mode: {settings.DEBUG}")
        self.logger.info(f"Database URL: {settings.DATABASE_URL.split('@')[1] if '@' in settings.DATABASE_URL else 'configured'}")
        self.logger.info(f"Gemini API Key: {'configured' if settings.GEMINI_API_KEY else 'not configured'}")
        self.logger.info(f"Langfuse: {'configured' if settings.LANGFUSE_PUBLIC_KEY else 'not configured'}")
        self.logger.info("=" * 80)
    
    def _initialize_langfuse(self):
        """Initialize Langfuse for tracing"""
        if settings.LANGFUSE_PUBLIC_KEY and settings.LANGFUSE_SECRET_KEY:
            try:
                self.langfuse = Langfuse(
                    public_key=settings.LANGFUSE_PUBLIC_KEY,
                    secret_key=settings.LANGFUSE_SECRET_KEY,
                    host=settings.LANGFUSE_HOST
                )
                self.logger.info("✅ Langfuse initialized successfully")
            except Exception as e:
                self.logger.error(f"❌ Failed to initialize Langfuse: {str(e)}")
        else:
            self.logger.info("ℹ️ Langfuse not configured - tracing disabled")
    
    def trace_operation(self, operation_name: str, **kwargs):
        """Trace an operation with Langfuse"""
        if self.langfuse:
            try:
                trace = self.langfuse.trace(name=operation_name, **kwargs)
                return trace
            except Exception as e:
                self.logger.error(f"Failed to create trace: {str(e)}")
        return None
    
    def log_info(self, message: str, **kwargs):
        """Log info message with context"""
        if kwargs:
            context = " | ".join([f"{k}={v}" for k, v in kwargs.items()])
            self.logger.info(f"{message} | {context}")
        else:
            self.logger.info(message)
    
    def log_error(self, message: str, **kwargs):
        """Log error message with context"""
        if kwargs:
            context = " | ".join([f"{k}={v}" for k, v in kwargs.items()])
            self.logger.error(f"{message} | {context}")
        else:
            self.logger.error(message)
    
    def log_warning(self, message: str, **kwargs):
        """Log warning message with context"""
        if kwargs:
            context = " | ".join([f"{k}={v}" for k, v in kwargs.items()])
            self.logger.warning(f"{message} | {context}")
        else:
            self.logger.warning(message)
    
    def log_debug(self, message: str, **kwargs):
        """Log debug message with context"""
        if kwargs:
            context = " | ".join([f"{k}={v}" for k, v in kwargs.items()])
            self.logger.debug(f"{message} | {context}")
        else:
            self.logger.debug(message)
    
    def log_performance(self, operation: str, duration: float, **kwargs):
        """Log performance metrics"""
        context = " | ".join([f"{k}={v}" for k, v in kwargs.items()])
        self.logger.info(f"⏱️ PERFORMANCE | {operation} | duration={duration:.3f}s | {context}")
    
    def log_database_operation(self, operation: str, table: str = None, **kwargs):
        """Log database operations"""
        context_parts = []
        if table:
            context_parts.append(f"table={table}")
        context_parts.extend([f"{k}={v}" for k, v in kwargs.items()])
        context = " | ".join(context_parts)
        self.logger.info(f"🗄️ DATABASE | {operation} | {context}")
    
    def log_ai_operation(self, operation: str, model: str = None, **kwargs):
        """Log AI operations"""
        context_parts = []
        if model:
            context_parts.append(f"model={model}")
        context_parts.extend([f"{k}={v}" for k, v in kwargs.items()])
        context = " | ".join(context_parts)
        self.logger.info(f"🤖 AI | {operation} | {context}")
    
    def log_user_action(self, action: str, **kwargs):
        """Log user actions"""
        context = " | ".join([f"{k}={v}" for k, v in kwargs.items()])
        self.logger.info(f"👤 USER | {action} | {context}")
    
    def log_exception(self, exception: Exception, context: str = None):
        """Log exceptions with full traceback"""
        if context:
            self.logger.error(f"💥 EXCEPTION | {context} | {type(exception).__name__}: {str(exception)}")
        else:
            self.logger.error(f"💥 EXCEPTION | {type(exception).__name__}: {str(exception)}")
        
        # Log full traceback in debug mode
        if settings.DEBUG:
            import traceback
            self.logger.debug(f"Full traceback:\n{traceback.format_exc()}")
    
    def log_workflow_step(self, workflow: str, step: str, status: str, **kwargs):
        """Log workflow steps"""
        context = " | ".join([f"{k}={v}" for k, v in kwargs.items()])
        status_emoji = {"start": "🔄", "success": "✅", "error": "❌", "warning": "⚠️"}.get(status, "ℹ️")
        self.logger.info(f"{status_emoji} WORKFLOW | {workflow} | {step} | {status} | {context}")

# Global observability manager
observability = ObservabilityManager()

