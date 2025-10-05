#!/usr/bin/env python3
"""
Test script to verify the data generation fixes are working
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from core.database_manager import DatabaseManager
from core.data_generation_orchestrator import DataGenerationOrchestrator
import pandas as pd

def test_database_manager():
    """Test the database manager with our fixes"""
    print("🧪 Testing DatabaseManager fixes...")
    
    # Initialize database manager
    db_manager = DatabaseManager()
    
    # Test the verify_schema_match method (our new fix)
    try:
        schema_info = {
            'test_table': {
                'columns': [
                    {'name': 'id', 'type': 'integer'},
                    {'name': 'name', 'type': 'varchar'}
                ]
            }
        }
        result = db_manager.verify_schema_match(schema_info)
        print("✅ verify_schema_match method works:", result)
    except Exception as e:
        print("❌ verify_schema_match method failed:", e)
    
    # Test DDL execution with drop_existing
    test_ddl = """
    CREATE TABLE test_customers (
        customer_id SERIAL PRIMARY KEY,
        first_name VARCHAR(50) NOT NULL,
        last_name VARCHAR(50) NOT NULL,
        email VARCHAR(100) UNIQUE NOT NULL
    );
    """
    
    try:
        print("🧪 Testing DDL execution with drop_existing=True...")
        success = db_manager.create_tables_from_ddl(test_ddl, drop_existing=True)
        if success:
            print("✅ DDL execution with drop_existing=True succeeded")
        else:
            print("❌ DDL execution with drop_existing=True failed")
    except Exception as e:
        print("❌ DDL execution failed:", e)
    
    # Test data insertion
    try:
        print("🧪 Testing data insertion...")
        test_data = pd.DataFrame({
            'customer_id': [1, 2, 3],
            'first_name': ['John', 'Jane', 'Bob'],
            'last_name': ['Doe', 'Smith', 'Johnson'],
            'email': ['john@example.com', 'jane@example.com', 'bob@example.com']
        })
        
        success = db_manager.insert_dataframe('test_customers', test_data)
        if success:
            print("✅ Data insertion succeeded")
        else:
            print("❌ Data insertion failed")
    except Exception as e:
        print("❌ Data insertion failed:", e)
    
    # Test second insertion (should fail with our old bug, but work with our fix)
    try:
        print("🧪 Testing second data insertion (should work with our fix)...")
        test_data2 = pd.DataFrame({
            'customer_id': [4, 5, 6],
            'first_name': ['Alice', 'Charlie', 'Diana'],
            'last_name': ['Brown', 'Wilson', 'Davis'],
            'email': ['alice@example.com', 'charlie@example.com', 'diana@example.com']
        })
        
        success = db_manager.insert_dataframe('test_customers', test_data2)
        if success:
            print("✅ Second data insertion succeeded - our fix is working!")
        else:
            print("❌ Second data insertion failed")
    except Exception as e:
        print("❌ Second data insertion failed:", e)

def test_data_generation_orchestrator():
    """Test the data generation orchestrator"""
    print("\n🧪 Testing DataGenerationOrchestrator...")
    
    try:
        orchestrator = DataGenerationOrchestrator()
        print("✅ DataGenerationOrchestrator initialized successfully")
    except Exception as e:
        print("❌ DataGenerationOrchestrator initialization failed:", e)

if __name__ == "__main__":
    print("🚀 Starting data generation fixes test...")
    test_database_manager()
    test_data_generation_orchestrator()
    print("\n✅ Test completed!")
