"""
Authentication UI components for Streamlit.

This module handles all UI-related authentication logic,
separated from business logic for better maintainability.
"""

import logging
import streamlit as st
from typing import Optional, Dict, Any
from .auth_strategies import AuthenticationError

logger = logging.getLogger(__name__)


class AuthenticationUI:
    """Handles authentication UI components and interactions"""
    
    def __init__(self):
        self._initialize_session_state()
    
    def _initialize_session_state(self):
        """Initialize authentication-related session state"""
        if 'auth_status' not in st.session_state:
            st.session_state.auth_status = {
                'authenticated': False,
                'client': None,
                'method': None
            }
    
    def display_authentication_status(self, auth_status: Dict[str, Any]) -> None:
        """Display current authentication status"""
        if auth_status["authenticated"]:
            method_display = self._format_method_name(auth_status['method'])
            st.success(f"✅ Authenticated via {method_display}")
        else:
            st.warning("⚠️ Authentication required")
    
    def display_authentication_error(self, error: AuthenticationError) -> None:
        """Display authentication error with appropriate styling"""
        error_icon = self._get_error_icon(error.method)
        st.error(f"{error_icon} {error}")
    
    def display_authentication_success(self, method: str) -> None:
        """Display authentication success message"""
        method_display = self._format_method_name(method)
        st.success(f"✅ Authenticated via {method_display}")
    
    def get_manual_api_key_input(self) -> Optional[str]:
        """Get API key from manual user input"""
        st.info("💡 Enter your Gemini API key for authentication")
        
        api_key = st.text_input(
            "Gemini API Key", 
            type="password",
            help="Enter your Gemini API key for authentication",
            placeholder="Enter your API key here..."
        )
        
        if api_key and st.button("🔐 Authenticate"):
            return api_key
        
        return None
    
    def display_authentication_info(self, available_methods: list[str]) -> None:
        """Display information about available authentication methods"""
        if not available_methods:
            st.error("❌ No authentication methods available")
            return
        
        if "vertex_ai_adc" in available_methods:
            st.info("💡 Vertex AI authentication available - will be used automatically")
        elif "env_api_key" in available_methods:
            st.info("💡 Environment API key available - will be used automatically")
        else:
            st.info("💡 Please provide an API key for authentication")
    
    def _format_method_name(self, method: str) -> str:
        """Format authentication method name for display"""
        if not method:
            return "Unknown"
        return method.replace('_', ' ').title()
    
    def _get_error_icon(self, method: str) -> str:
        """Get appropriate error icon based on authentication method"""
        icon_map = {
            "vertex_ai_adc": "🔧",
            "api_key": "🔑",
            "env_api_key": "⚙️"
        }
        return icon_map.get(method, "❌")
    
    def update_auth_status(self, authenticated: bool, client: Optional[Any], method: Optional[str]) -> None:
        """Update authentication status in session state"""
        self._initialize_session_state()
        st.session_state.auth_status.update({
            'authenticated': authenticated,
            'client': client,
            'method': method
        })
    
    def get_auth_status(self) -> Dict[str, Any]:
        """Get current authentication status"""
        self._initialize_session_state()
        return st.session_state.auth_status.copy()
    
    def is_authenticated(self) -> bool:
        """Check if user is currently authenticated"""
        auth_status = self.get_auth_status()
        return auth_status.get('authenticated', False)
    
    def get_authenticated_client(self) -> Optional[Any]:
        """Get the authenticated client if available"""
        auth_status = self.get_auth_status()
        if auth_status.get('authenticated', False):
            return auth_status.get('client')
        return None
    
    def clear_auth_status(self) -> None:
        """Clear authentication status (useful for testing)"""
        self._initialize_session_state()
        st.session_state.auth_status.update({
            'authenticated': False,
            'client': None,
            'method': None
        })
