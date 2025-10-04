"""
Authentication Manager for Gemini AI.

This module provides a clean authentication approach using the Strategy pattern
to eliminate duplications and improve maintainability.
"""

import logging
import streamlit as st
from typing import Optional, Dict, Any
from google import genai
from .auth_strategies import (
    AuthenticationStrategyFactory, 
    AuthenticationError,
    APIKeyAuthStrategy,
    VertexAIAuthStrategy
)
from .auth_ui import AuthenticationUI

logger = logging.getLogger(__name__)

class AuthManager:
    """Authentication manager for Gemini AI using Strategy pattern"""
    
    def __init__(self):
        self.ui = AuthenticationUI()
        self.strategies = AuthenticationStrategyFactory.get_strategies()
    
    def get_authentication_status(self) -> Dict[str, Any]:
        """Get current authentication status"""
        return self.ui.get_auth_status()
    
    def get_gemini_client(self) -> Optional[genai.Client]:
        """Get Gemini client using the configured authentication method"""
        try:
            # Get the single strategy based on environment configuration
            strategies = self.strategies
            
            if not strategies:
                logger.error("No authentication strategy available")
                self.ui.update_auth_status(False, None, None)
                return None
            
            strategy = strategies[0]  # Only one strategy at a time
            
            if not strategy.is_available():
                logger.error(f"Authentication strategy {strategy.get_method_name()} is not available")
                self.ui.update_auth_status(False, None, None)
                return None
            
            # Try authentication with the single strategy
            client = self._authenticate_with_strategy(strategy)
            return client
            
        except AuthenticationError as e:
            logger.error(f"Authentication failed: {e}")
            self.ui.update_auth_status(False, None, None)
            return None
        except Exception as e:
            logger.error(f"Unexpected authentication error: {str(e)}")
            self.ui.update_auth_status(False, None, None)
            return None
    
    def authenticate_with_api_key(self, api_key: str) -> Optional[genai.Client]:
        """Authenticate using a specific API key (for manual UI input)"""
        try:
            return self._authenticate_with_strategy(APIKeyAuthStrategy(), api_key=api_key)
        except AuthenticationError as e:
            logger.error(f"API key authentication failed: {e}")
            return None
    
    def _authenticate_with_strategy(self, strategy, **kwargs) -> Optional[genai.Client]:
        """Authenticate using a specific strategy"""
        try:
            client = strategy.authenticate(**kwargs)
            if client:
                self.ui.update_auth_status(True, client, strategy.get_method_name())
                logger.info(f"Authentication successful with {strategy.get_method_name()}")
            return client
        except AuthenticationError as e:
            logger.error(f"Authentication failed with {strategy.get_method_name()}: {e}")
            raise
    
    
    def validate_current_auth(self) -> bool:
        """Validate if current authentication is still valid"""
        auth_status = self.ui.get_auth_status()
        
        if not auth_status.get('authenticated', False) or not auth_status.get('client'):
            return False
        
        try:
            # Validation - check if client exists
            return auth_status['client'] is not None
        except Exception:
            # Authentication is no longer valid
            self.ui.update_auth_status(False, None, None)
            return False
    
    def get_authentication_ui(self) -> Optional[str]:
        """Get authentication UI component"""
        logger.info("Rendering authentication UI...")
        auth_status = self.get_authentication_status()
        
        # Check if user is already authenticated
        if auth_status["authenticated"]:
            self.ui.display_authentication_status(auth_status)
            return None
        
        # Try automatic authentication first
        if self._try_automatic_authentication():
            return None
        
        # Show manual authentication if automatic failed
        self._show_manual_authentication()
        return None
    
    def _try_automatic_authentication(self) -> bool:
        """Try automatic authentication with the configured method"""
        if not self.strategies:
            return False
        
        strategy = self.strategies[0]
        
        # Display info about the authentication method
        method_name = strategy.get_method_name()
        self.ui.display_authentication_info([method_name])
        
        # Try authentication
        try:
            client = self.get_gemini_client()
            if client:
                auth_status = self.get_authentication_status()
                self.ui.display_authentication_success(auth_status['method'])
                return True
        except Exception as e:
            logger.error(f"Automatic authentication failed: {str(e)}")
        
        return False
    
    def _show_manual_authentication(self) -> None:
        """Show manual authentication UI (only for Vertex AI fallback)"""
        logger.info("Showing manual authentication input")
        
        # Only show manual API key input if we're using Vertex AI
        # (API key users already have their key in environment)
        if self.strategies and isinstance(self.strategies[0], VertexAIAuthStrategy):
            api_key = self.ui.get_manual_api_key_input()
            if api_key:
                client = self.authenticate_with_api_key(api_key)
                if client:
                    logger.info("Manual authentication successful")
                    self.ui.display_authentication_success("api_key")
                    st.rerun()
                else:
                    logger.warning("Manual authentication failed")
        else:
            # For API key users, just show error message
            st.error("❌ API key authentication failed. Please check your GEMINI_API_KEY environment variable.")

# Global authentication manager
auth_manager = AuthManager()
