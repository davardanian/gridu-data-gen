#!/usr/bin/env python3
"""
Test script to verify Langfuse integration works correctly
"""
import os
import sys
import time
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add the project root to Python path
sys.path.insert(0, '/home/davardanian/github/personal/gridu-data-gen')

def test_langfuse_integration():
    """Test the Langfuse integration"""
    print("🧪 Testing Langfuse Integration...")
    
    try:
        # Import the observability manager
        from core.observability import observability
        
        print(f"✅ Observability manager imported successfully")
        print(f"✅ Langfuse configured: {observability.langfuse is not None}")
        
        if not observability.langfuse:
            print("❌ Langfuse client is None - check your credentials")
            return False
        
        # Test 1: Create a trace
        print("\n📝 Test 1: Creating a trace...")
        trace = observability.create_trace(
            name="test_trace",
            user_id="test_user",
            session_id="test_session",
            input_data={"test": "data"},
            metadata={"test_metadata": "value"},
            tags=["test", "integration"]
        )
        
        if trace:
            print("✅ Trace created successfully")
        else:
            print("❌ Failed to create trace")
            return False
        
        # Test 2: Create a generation
        print("\n🤖 Test 2: Creating a generation...")
        generation = observability.create_generation(
            span=trace,
            name="test_generation",
            model="test-model",
            input_data="Test input",
            output_data="Test output",
            metadata={"test_gen_metadata": "value"}
        )
        
        if generation:
            print("✅ Generation created successfully")
        else:
            print("❌ Failed to create generation")
            return False
        
        # Test 3: Update trace
        print("\n🔄 Test 3: Updating trace...")
        try:
            trace.update_trace(
                output="Test completed successfully",
                metadata={"completion_status": "success"}
            )
            print("✅ Trace updated successfully")
        except Exception as e:
            print(f"❌ Failed to update trace: {e}")
            return False
        
        # Test 4: Flush traces
        print("\n📤 Test 4: Flushing traces...")
        try:
            observability.flush_traces()
            print("✅ Traces flushed successfully")
        except Exception as e:
            print(f"❌ Failed to flush traces: {e}")
            return False
        
        print("\n🎉 All tests passed! Langfuse integration is working correctly.")
        return True
        
    except Exception as e:
        print(f"❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_langfuse_integration()
    sys.exit(0 if success else 1)
