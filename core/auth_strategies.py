"""
Authentication strategies for different authentication methods.

This module implements the Strategy pattern for authentication,
allowing easy extension and testing of different auth methods.
"""

import logging
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any
from google import genai
from config.settings import settings

logger = logging.getLogger(__name__)


class AuthenticationError(Exception):
    """Custom exception for authentication errors"""
    
    def __init__(self, message: str, method: str, original_error: Optional[Exception] = None):
        self.method = method
        self.original_error = original_error
        super().__init__(message)


class AuthenticationStrategy(ABC):
    """Abstract base class for authentication strategies"""
    
    @abstractmethod
    def authenticate(self, **kwargs) -> Optional[genai.Client]:
        """Authenticate and return a Gemini client"""
        pass
    
    @abstractmethod
    def get_method_name(self) -> str:
        """Get the name of this authentication method"""
        pass
    
    @abstractmethod
    def is_available(self) -> bool:
        """Check if this authentication method is available"""
        pass


class VertexAIAuthStrategy(AuthenticationStrategy):
    """Authentication strategy using Vertex AI with Google ADC"""
    
    def authenticate(self, **kwargs) -> Optional[genai.Client]:
        """Authenticate using Vertex AI"""
        try:
            logger.info("Attempting Vertex AI authentication...")
            client = genai.Client(
                vertexai=True,
                project=settings.PROJECT_ID,
                location=settings.LOCATION
            )
            logger.info("Vertex AI authentication successful")
            return client
        except Exception as e:
            logger.error(f"Vertex AI authentication failed: {str(e)}")
            raise AuthenticationError(
                f"Vertex AI authentication failed: {str(e)}",
                "vertex_ai_adc",
                e
            )
    
    def get_method_name(self) -> str:
        return "vertex_ai_adc"
    
    def is_available(self) -> bool:
        """Check if Vertex AI authentication is available"""
        try:
            from google.auth import default
            credentials, project = default()
            return credentials is not None and project is not None
        except Exception as e:
            logger.debug(f"Vertex AI not available: {str(e)}")
            return False


class APIKeyAuthStrategy(AuthenticationStrategy):
    """Authentication strategy using API key"""
    
    def authenticate(self, api_key: str, **kwargs) -> Optional[genai.Client]:
        """Authenticate using API key"""
        if not api_key:
            raise AuthenticationError("API key is required", "api_key")
        
        try:
            logger.info("Attempting API key authentication...")
            client = genai.Client(api_key=api_key)
            
            # Validate the API key by making a test call
            logger.info("Validating API key with test call...")
            test_response = client.models.list()
            logger.info("API key authentication successful")
            return client
        except Exception as e:
            logger.error(f"API key authentication failed: {str(e)}")
            raise AuthenticationError(
                f"Invalid API key: {str(e)}",
                "api_key",
                e
            )
    
    def get_method_name(self) -> str:
        return "api_key"
    
    def is_available(self) -> bool:
        """API key authentication is always available if key is provided"""
        return True


class EnvironmentAPIKeyAuthStrategy(AuthenticationStrategy):
    """Authentication strategy using environment variable API key"""
    
    def authenticate(self, **kwargs) -> Optional[genai.Client]:
        """Authenticate using environment API key"""
        if not settings.GEMINI_API_KEY:
            raise AuthenticationError("Environment API key not configured", "env_api_key")
        
        try:
            logger.info("Attempting environment API key authentication...")
            client = genai.Client(api_key=settings.GEMINI_API_KEY)
            
            # Validate the API key by making a test call
            logger.info("Validating API key with test call...")
            test_response = client.models.list()
            logger.info("Environment API key authentication successful")
            return client
        except Exception as e:
            logger.error(f"Environment API key authentication failed: {str(e)}")
            raise AuthenticationError(
                f"Environment API key is invalid: {str(e)}",
                "env_api_key",
                e
            )
    
    def get_method_name(self) -> str:
        return "env_api_key"
    
    def is_available(self) -> bool:
        """Check if environment API key is available"""
        return bool(settings.GEMINI_API_KEY)


class AuthenticationStrategyFactory:
    """Factory for creating authentication strategies"""
    
    @staticmethod
    def get_strategies() -> list[AuthenticationStrategy]:
        """Get authentication strategies based on environment configuration"""
        if settings.GEMINI_API_KEY:
            # If API key is provided, use only API key authentication
            return [EnvironmentAPIKeyAuthStrategy()]
        else:
            # If no API key, use only Vertex AI authentication
            return [VertexAIAuthStrategy()]
    
    @staticmethod
    def get_available_strategies() -> list[AuthenticationStrategy]:
        """Get only available authentication strategies"""
        return [strategy for strategy in AuthenticationStrategyFactory.get_strategies() 
                if strategy.is_available()]
