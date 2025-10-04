# core/guardrails.py
import re
from typing import List, Dict, Any, Optional
from core.observability import observability

class GuardrailsManager:
    """Manages security guardrails and input validation"""
    
    def __init__(self):
        self.observability = observability
        self.injection_patterns = self._load_injection_patterns()
        self.pii_patterns = self._load_pii_patterns()
    
    def _load_injection_patterns(self) -> List[str]:
        """Load prompt injection detection patterns"""
        return [
            r'ignore\s+(?:previous|above|all)\s+(?:instructions?|prompts?)',
            r'forget\s+(?:everything|all|previous)',
            r'you\s+are\s+now\s+(?:a\s+)?(?:different|new)',
            r'pretend\s+to\s+be',
            r'act\s+as\s+if',
            r'roleplay\s+as',
            r'system\s*:\s*',
            r'<\|.*?\|>',
            r'\[.*?\]',
            r'\{.*?\}',
            r'override\s+(?:system|safety)',
            r'jailbreak',
            r'bypass\s+(?:safety|guardrails)',
            r'admin\s+(?:mode|access)',
            r'developer\s+(?:mode|access)',
            r'debug\s+(?:mode|access)',
        ]
    
    def _load_pii_patterns(self) -> Dict[str, str]:
        """Load PII detection patterns"""
        return {
            'email': r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
            'phone': r'\b(?:\+?1[-.\s]?)?\(?[0-9]{3}\)?[-.\s]?[0-9]{3}[-.\s]?[0-9]{4}\b',
            'ssn': r'\b\d{3}-?\d{2}-?\d{4}\b',
            'credit_card': r'\b\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}\b',
            'ip_address': r'\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b',
        }
    
    def validate_input(self, user_input: str, input_type: str = "general") -> Dict[str, Any]:
        """Validate user input for security and safety"""
        validation_result = {
            "is_valid": True,
            "warnings": [],
            "errors": [],
            "sanitized_input": user_input
        }
        
        try:
            # Check for prompt injection
            injection_result = self._check_prompt_injection(user_input)
            if not injection_result["is_safe"]:
                validation_result["is_valid"] = False
                validation_result["errors"].extend(injection_result["threats"])
                self.observability.log_error("Prompt injection detected", 
                                           input=user_input[:100],
                                           threats=injection_result["threats"])
            
            # Check for PII
            pii_result = self._check_pii(user_input)
            if pii_result["has_pii"]:
                validation_result["warnings"].extend(pii_result["pii_types"])
                validation_result["sanitized_input"] = self._sanitize_pii(user_input)
                self.observability.log_info("PII detected and sanitized", 
                                          pii_types=pii_result["pii_types"])
            
            # Check input length
            if len(user_input) > 10000:
                validation_result["warnings"].append("Input is very long, may cause performance issues")
            
            # Check for SQL injection (if input type is query)
            if input_type == "query":
                sql_injection_result = self._check_sql_injection(user_input)
                if not sql_injection_result["is_safe"]:
                    validation_result["is_valid"] = False
                    validation_result["errors"].extend(sql_injection_result["threats"])
            
            return validation_result
            
        except Exception as e:
            self.observability.log_error(f"Input validation failed: {str(e)}")
            validation_result["is_valid"] = False
            validation_result["errors"].append("Validation error occurred")
            return validation_result
    
    def _check_prompt_injection(self, user_input: str) -> Dict[str, Any]:
        """Check for prompt injection attempts"""
        result = {
            "is_safe": True,
            "threats": []
        }
        
        user_input_lower = user_input.lower()
        
        for pattern in self.injection_patterns:
            if re.search(pattern, user_input_lower, re.IGNORECASE):
                result["is_safe"] = False
                result["threats"].append(f"Potential prompt injection: {pattern}")
        
        return result
    
    def _check_pii(self, user_input: str) -> Dict[str, Any]:
        """Check for personally identifiable information"""
        result = {
            "has_pii": False,
            "pii_types": []
        }
        
        for pii_type, pattern in self.pii_patterns.items():
            if re.search(pattern, user_input, re.IGNORECASE):
                result["has_pii"] = True
                result["pii_types"].append(pii_type)
        
        return result
    
    def _sanitize_pii(self, user_input: str) -> str:
        """Sanitize PII from user input"""
        sanitized = user_input
        
        for pii_type, pattern in self.pii_patterns.items():
            if pii_type == 'email':
                sanitized = re.sub(pattern, '[EMAIL_REDACTED]', sanitized, flags=re.IGNORECASE)
            elif pii_type == 'phone':
                sanitized = re.sub(pattern, '[PHONE_REDACTED]', sanitized, flags=re.IGNORECASE)
            elif pii_type == 'ssn':
                sanitized = re.sub(pattern, '[SSN_REDACTED]', sanitized, flags=re.IGNORECASE)
            elif pii_type == 'credit_card':
                sanitized = re.sub(pattern, '[CARD_REDACTED]', sanitized, flags=re.IGNORECASE)
            elif pii_type == 'ip_address':
                sanitized = re.sub(pattern, '[IP_REDACTED]', sanitized, flags=re.IGNORECASE)
        
        return sanitized
    
    def _check_sql_injection(self, user_input: str) -> Dict[str, Any]:
        """Check for SQL injection attempts"""
        result = {
            "is_safe": True,
            "threats": []
        }
        
        # SQL injection patterns
        sql_patterns = [
            r'union\s+select',
            r'drop\s+table',
            r'delete\s+from',
            r'insert\s+into',
            r'update\s+set',
            r';\s*drop',
            r';\s*delete',
            r';\s*insert',
            r';\s*update',
            r'--\s*',
            r'/\*.*?\*/',
            r'xp_cmdshell',
            r'sp_executesql',
        ]
        
        user_input_lower = user_input.lower()
        
        for pattern in sql_patterns:
            if re.search(pattern, user_input_lower, re.IGNORECASE):
                result["is_safe"] = False
                result["threats"].append(f"Potential SQL injection: {pattern}")
        
        return result
    
    def validate_sql_query(self, sql_query: str) -> Dict[str, Any]:
        """Validate SQL query for safety and correctness"""
        result = {
            "is_valid": True,
            "warnings": [],
            "errors": []
        }
        
        try:
            sql_upper = sql_query.upper().strip()
            
            # Check for dangerous operations
            dangerous_operations = ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'ALTER', 'CREATE', 'TRUNCATE']
            for operation in dangerous_operations:
                if operation in sql_upper:
                    result["is_valid"] = False
                    result["errors"].append(f"Dangerous operation detected: {operation}")
            
            # Must start with SELECT
            if not sql_upper.startswith('SELECT'):
                result["is_valid"] = False
                result["errors"].append("Query must start with SELECT")
            
            # Check for potential performance issues
            if 'SELECT *' in sql_upper:
                result["warnings"].append("Using SELECT * may impact performance")
            
            # Check for missing LIMIT on large tables
            if 'LIMIT' not in sql_upper and 'COUNT' not in sql_upper:
                result["warnings"].append("Consider adding LIMIT clause for large result sets")
            
            return result
            
        except Exception as e:
            self.observability.log_error(f"SQL validation failed: {str(e)}")
            result["is_valid"] = False
            result["errors"].append("SQL validation error")
            return result
    
    def sanitize_output(self, output: str) -> str:
        """Sanitize AI output for display"""
        try:
            # Remove potential script tags
            output = re.sub(r'<script.*?</script>', '[SCRIPT_REMOVED]', output, flags=re.IGNORECASE | re.DOTALL)
            
            # Remove potential HTML tags that could be dangerous
            output = re.sub(r'<iframe.*?</iframe>', '[IFRAME_REMOVED]', output, flags=re.IGNORECASE | re.DOTALL)
            
            # Remove potential JavaScript
            output = re.sub(r'javascript:', '[JAVASCRIPT_REMOVED]', output, flags=re.IGNORECASE)
            
            return output
            
        except Exception as e:
            self.observability.log_error(f"Output sanitization failed: {str(e)}")
            return output
    
    def log_security_event(self, event_type: str, details: Dict[str, Any]):
        """Log security events for monitoring"""
        self.observability.log_info(f"Security event: {event_type}", **details)

