#!/usr/bin/env python3
"""
Debug script to inspect Langfuse client methods
"""
import os
import sys
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add the project root to Python path
sys.path.insert(0, '/home/davardanian/github/personal/gridu-data-gen')

def debug_langfuse_client():
    """Debug the Langfuse client to see available methods"""
    print("🔍 Debugging Langfuse Client...")
    
    try:
        from core.observability import observability
        
        if not observability.langfuse:
            print("❌ Langfuse client is None")
            return
        
        client = observability.langfuse
        print(f"✅ Langfuse client type: {type(client)}")
        print(f"✅ Langfuse client: {client}")
        
        # Get all available methods
        methods = [method for method in dir(client) if not method.startswith('_')]
        print(f"\n📋 Available methods on Langfuse client:")
        for method in sorted(methods):
            print(f"  - {method}")
        
        # Check if specific methods exist
        methods_to_check = [
            'trace', 'start_as_current_span', 'start_as_current_generation',
            'create_trace', 'update_current_trace', 'flush', 'shutdown'
        ]
        
        print(f"\n🔍 Checking specific methods:")
        for method in methods_to_check:
            has_method = hasattr(client, method)
            print(f"  - {method}: {'✅' if has_method else '❌'}")
        
        # Try to get the client using get_client if available
        try:
            from langfuse import get_client
            alt_client = get_client()
            print(f"\n🔄 Alternative client type: {type(alt_client)}")
            alt_methods = [method for method in dir(alt_client) if not method.startswith('_')]
            print(f"📋 Available methods on alternative client:")
            for method in sorted(alt_methods):
                print(f"  - {method}")
        except Exception as e:
            print(f"❌ Could not get alternative client: {e}")
        
    except Exception as e:
        print(f"❌ Debug failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_langfuse_client()
