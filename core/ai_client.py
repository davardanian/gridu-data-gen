"""
AI client - no overengineering.

This module provides a straightforward interface to Gemini AI.
"""

import streamlit as st
from typing import Optional
from langfuse import observe, get_client
from core.observability import observability


class AIClient:
    """AI client - just makes API calls."""
    
    def __init__(self):
        # Enable debug logging by default for troubleshooting
        self.debug_mode = True
    
    @observe(as_type="generation", name="gemini_content_generation")
    def generate_content(self, prompt: str, temperature: float = 0.4, 
                        max_tokens: int = None) -> Optional[str]:
        """Generate content using Gemini AI."""
        try:
            # Get Langfuse client for enhanced tracing
            langfuse = get_client()
            
            # Update observation with model info
            from config.settings import settings
            model_name = settings.GEMINI_MODEL
            
            # Update current generation with comprehensive metadata
            if langfuse:
                langfuse.update_current_generation(
                    model=model_name,
                    model_parameters={
                        "temperature": temperature,
                        "max_tokens": max_tokens or settings.MAX_OUTPUT_TOKENS
                    },
                    input=prompt[:500] + "..." if len(prompt) > 500 else prompt,
                    metadata={
                        "prompt_length": len(prompt),
                        "model": model_name,
                        "temperature": temperature,
                        "max_tokens": max_tokens or settings.MAX_OUTPUT_TOKENS
                    }
                )
            
            # Get Gemini client
            client = self._get_gemini_client()
            if not client:
                st.error("❌ No Gemini client available")
                observability.update_current_observation(
                    output="Error: No Gemini client available",
                    level="ERROR"
                )
                return None
            
            # Use settings default if max_tokens not provided
            if max_tokens is None:
                max_tokens = settings.MAX_OUTPUT_TOKENS
            
            # Make API call
            from google.genai import types
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
                
                # Extract token usage if available
                usage_details = {}
                if hasattr(response, 'usage_metadata'):
                    usage_metadata = response.usage_metadata
                    usage_details = {
                        "input_tokens": getattr(usage_metadata, 'prompt_token_count', 0),
                        "output_tokens": getattr(usage_metadata, 'candidates_token_count', 0),
                        "total_tokens": getattr(usage_metadata, 'total_token_count', 0)
                    }
                
                # Update generation with response and usage details
                if langfuse:
                    langfuse.update_current_generation(
                        output=response_text[:500] + "..." if len(response_text) > 500 else response_text,
                        usage_details=usage_details if usage_details else None,
                        metadata={
                            "response_length": len(response_text),
                            "success": True,
                            "has_usage_data": bool(usage_details)
                        }
                    )
                
                # Log the raw response for debugging if debug mode is enabled
                if self.debug_mode:
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
                if langfuse:
                    langfuse.update_current_generation(
                        output="Error: Invalid response from AI",
                        level="ERROR",
                        status_message="Invalid response from AI"
                    )
                return None
                
        except Exception as e:
            st.error(f"❌ AI generation failed: {str(e)}")
            if langfuse:
                langfuse.update_current_generation(
                    output=f"Error: {str(e)}",
                    level="ERROR",
                    status_message=f"AI generation failed: {str(e)}"
                )
            observability.log_exception(e, "ai_client_generate_content")
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
    