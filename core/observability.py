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
    
    def create_trace(self, name: str, user_id: str = None, session_id: str = None, 
                    input_data: dict = None, metadata: dict = None, tags: list = None):
        """Create a new Langfuse trace with proper structure"""
        if not self.langfuse:
            return None
            
        try:
            # Use start_span for manual span creation in Langfuse 3.x
            # This creates a span that we can manage manually
            trace = self.langfuse.start_span(
                name=name,
                input=input_data,
                metadata=metadata or {}
            )
            
            # Set trace-level attributes using update_trace
            if user_id or session_id or tags:
                trace.update_trace(
                    user_id=user_id,
                    session_id=session_id,
                    tags=tags
                )
            
            self.logger.info(f"✅ Created Langfuse trace: {name}")
            return trace
        except Exception as e:
            self.logger.error(f"❌ Failed to create trace '{name}': {str(e)}")
            return None
    
    def create_generation(self, trace, name: str, model: str = None, 
                         input_data: str = None, output_data: str = None, 
                         metadata: dict = None):
        """Create a generation within a trace"""
        if not trace or not self.langfuse:
            return None
            
        try:
            # Use start_generation for manual generation creation in Langfuse 3.x
            generation = self.langfuse.start_generation(
                name=name,
                model=model,
                input=input_data,
                output=output_data,
                metadata=metadata or {}
            )
                
            self.logger.info(f"✅ Created generation: {name}")
            return generation
        except Exception as e:
            self.logger.error(f"❌ Failed to create generation '{name}': {str(e)}")
            return None
    
    def flush_traces(self):
        """Flush all pending traces to Langfuse"""
        if self.langfuse:
            try:
                self.langfuse.flush()
                self.logger.info("✅ Flushed traces to Langfuse")
            except Exception as e:
                self.logger.error(f"❌ Failed to flush traces: {str(e)}")
    
    def shutdown(self):
        """Shutdown Langfuse client and flush remaining traces"""
        if self.langfuse:
            try:
                self.langfuse.shutdown()
                self.logger.info("✅ Langfuse client shutdown complete")
            except Exception as e:
                self.logger.error(f"❌ Failed to shutdown Langfuse: {str(e)}")
    
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
    
    def get_langfuse_client(self):
        """Get the Langfuse client instance"""
        return self.langfuse
    
    def is_langfuse_enabled(self):
        """Check if Langfuse is properly configured and enabled"""
        return self.langfuse is not None
    
    def update_current_trace(self, **kwargs):
        """Update the current trace with additional information"""
        if self.langfuse:
            try:
                self.langfuse.update_current_trace(**kwargs)
                self.logger.debug(f"Updated current trace with: {kwargs}")
            except Exception as e:
                self.logger.error(f"Failed to update current trace: {str(e)}")
    
    def update_current_observation(self, **kwargs):
        """Update the current observation with additional information"""
        if self.langfuse:
            try:
                self.langfuse.update_current_span(**kwargs)
                self.logger.debug(f"Updated current observation with: {kwargs}")
            except Exception as e:
                self.logger.error(f"Failed to update current observation: {str(e)}")
    
    def score_current_trace(self, name: str, value: float, comment: str = None):
        """Score the current trace"""
        if self.langfuse:
            try:
                self.langfuse.score_current_trace(name=name, value=value, comment=comment)
                self.logger.info(f"Scored current trace: {name}={value}")
            except Exception as e:
                self.logger.error(f"Failed to score current trace: {str(e)}")
    
    def score_current_observation(self, name: str, value: float, comment: str = None):
        """Score the current observation"""
        if self.langfuse:
            try:
                self.langfuse.score_current_span(name=name, value=value, comment=comment)
                self.logger.info(f"Scored current observation: {name}={value}")
            except Exception as e:
                self.logger.error(f"Failed to score current observation: {str(e)}")

# Global observability manager
observability = ObservabilityManager()

