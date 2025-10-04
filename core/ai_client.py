"""
AI client - no overengineering.

This module provides a straightforward interface to Gemini AI.
"""

import streamlit as st
from typing import Optional


class AIClient:
    """AI client - just makes API calls."""
    
    def __init__(self):
        # Enable debug logging by default for troubleshooting
        self.debug_mode = True
    
    def generate_content(self, prompt: str, temperature: float = 0.4, 
                        max_tokens: int = None) -> Optional[str]:
        """Generate content using Gemini AI."""
        try:
            # Get Gemini client
            client = self._get_gemini_client()
            if not client:
                st.error("❌ No Gemini client available")
                return None
            
            # Use settings default if max_tokens not provided
            if max_tokens is None:
                from config.settings import settings
                max_tokens = settings.MAX_OUTPUT_TOKENS
            
            # Make API call
            from google.genai import types
            from config.settings import settings
            response = client.models.generate_content(
                model=settings.GEMINI_MODEL,
                contents=prompt,
                config=types.GenerateContentConfig(
                    temperature=temperature,
                    max_output_tokens=max_tokens
                )
            )
            
            if response and hasattr(response, 'text'):
                response_text = response.text
                # Log the raw response for debugging if debug mode is enabled
                if self.debug_mode:
                    from core.observability import observability
                    observability.log_info("=" * 80)
                    observability.log_info("RAW AI RESPONSE - generate_content")
                    observability.log_info("=" * 80)
                    observability.log_info(f"Response length: {len(response_text)} characters")
                    observability.log_info("-" * 80)
                    observability.log_info(f"Response text: {repr(response_text)}")
                    observability.log_info("-" * 80)
                    observability.log_info("RAW AI RESPONSE END")
                    observability.log_info("=" * 80)
                return response_text
            else:
                st.error("❌ Invalid response from AI")
                return None
                
        except Exception as e:
            st.error(f"❌ AI generation failed: {str(e)}")
            return None
    
    
    def _get_gemini_client(self):
        """Get Gemini client from authentication manager."""
        try:
            from core.auth_manager import auth_manager
            
            # Check if we have a valid authenticated client
            auth_status = auth_manager.get_authentication_status()
            
            if auth_status['authenticated'] and auth_status['client']:
                # Validate that the authentication is still valid
                if auth_manager.validate_current_auth():
                    return auth_status['client']
                else:
                    st.warning("Authentication expired. Please re-authenticate.")
                    return None
            
            # Try to get a new client
            client = auth_manager.get_gemini_client()
            
            if client:
                return client
            
            st.error("No valid authentication found. Please authenticate first.")
            return None
                
        except Exception as e:
            st.error(f"❌ Error getting Gemini client: {str(e)}")
            return None
    