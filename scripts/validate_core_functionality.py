#!/usr/bin/env python3
"""
Core Functionality Validation Script

This script validates that the core PySpark Tools functionality works correctly
without requiring all development dependencies.
"""

import sys
import time
from pathlib import Path

# Add the project root to the path
sys.path.insert(0, str(Path(__file__).parent.parent))

def test_sql_converter():
    """Test SQL to PySpark conversion functionality."""
    print("🔄 Testing SQL to PySpark conversion...")
    
    try:
        from pyspark_tools.sql_converter import SQLToPySparkConverter
        
        converter = SQLToPySparkConverter()
        
        # Test basic conversion
        basic_sql = "SELECT id, name FROM users WHERE age > 25"
        result = converter.convert_sql_to_pyspark(basic_sql)
        
        assert result.pyspark_code, "Basic conversion should produce PySpark code"
        assert result.dialect_used, "Should detect dialect"
        print("   ✅ Basic SQL conversion: PASSED")
        
        # Test complex conversion
        complex_sql = """
        WITH user_stats AS (
            SELECT user_id, COUNT(*) as order_count 
            FROM orders 
            GROUP BY user_id
        )
        SELECT * FROM user_stats WHERE order_count > 5
        """
        
        result = converter.convert_sql_to_pyspark(complex_sql)
        assert result.pyspark_code, "Complex conversion should produce PySpark code"
        assert len(result.optimizations) > 0, "Should provide optimizations"
        print("   ✅ Complex SQL conversion: PASSED")
        
        return True
        
    except Exception as e:
        print(f"   ❌ SQL conversion failed: {e}")
        return False

def test_memory_manager():
    """Test memory management functionality."""
    print("🗄️  Testing memory management...")
    
    try:
        from pyspark_tools.memory_manager import MemoryManager
        
        # Test in-memory database
        mm = MemoryManager(":memory:")
        
        # Test storing conversion
        mm.store_conversion(
            sql_query="SELECT * FROM test",
            pyspark_code="df = spark.table('test')",
            metadata={"test": True}
        )
        
        # Test retrieving conversions
        conversions = mm.get_recent_conversions(limit=1)
        assert len(conversions) == 1, "Should retrieve stored conversion"
        print("   ✅ Memory management: PASSED")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Memory management failed: {e}")
        return False

def test_batch_processor():
    """Test batch processing functionality."""
    print("📦 Testing batch processing...")
    
    try:
        from pyspark_tools.batch_processor import BatchProcessor
        from pyspark_tools.memory_manager import MemoryManager
        import tempfile
        
        mm = MemoryManager(":memory:")
        processor = BatchProcessor(mm)
        
        # Create temporary SQL files
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            # Create test files
            test_files = []
            for i in range(3):
                file_path = temp_path / f"query_{i}.sql"
                with open(file_path, 'w') as f:
                    f.write(f"SELECT {i} as id, 'test_{i}' as name FROM table_{i}")
                test_files.append(str(file_path))
            
            # Process files
            result = processor.process_files(test_files, str(temp_path / "output"))
            
            assert result['successful_conversions'] >= 2, "Should process most files successfully"
            assert result['total_files'] == 3, "Should process all files"
            print("   ✅ Batch processing: PASSED")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Batch processing failed: {e}")
        return False

def test_server_imports():
    """Test that server modules can be imported."""
    print("🖥️  Testing server imports...")
    
    try:
        from pyspark_tools.server import create_server
        
        # Just test that we can create the server object
        server = create_server()
        assert server is not None, "Should create server object"
        print("   ✅ Server imports: PASSED")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Server imports failed: {e}")
        return False

def test_performance():
    """Test basic performance requirements."""
    print("⚡ Testing performance...")
    
    try:
        from pyspark_tools.sql_converter import SQLToPySparkConverter
        
        converter = SQLToPySparkConverter()
        
        # Test conversion speed
        start_time = time.time()
        
        for _ in range(10):
            result = converter.convert_sql_to_pyspark(
                "SELECT id, name, email FROM users WHERE created_at > '2023-01-01' ORDER BY id"
            )
            assert result.pyspark_code, "Should produce code"
        
        total_time = time.time() - start_time
        avg_time = total_time / 10
        
        assert avg_time < 1.0, f"Average conversion time {avg_time:.3f}s should be <1s"
        print(f"   ✅ Performance: PASSED (avg: {avg_time:.3f}s per conversion)")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Performance test failed: {e}")
        return False

def main():
    """Run all core functionality tests."""
    print("🧪 PySpark Tools Core Functionality Validation")
    print("=" * 50)
    
    tests = [
        test_sql_converter,
        test_memory_manager,
        test_batch_processor,
        test_server_imports,
        test_performance
    ]
    
    passed = 0
    total = len(tests)
    
    for test_func in tests:
        if test_func():
            passed += 1
        print()  # Add spacing between tests
    
    print("=" * 50)
    print(f"📊 VALIDATION SUMMARY: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All core functionality tests PASSED!")
        print("✅ PySpark Tools is ready for use!")
        return 0
    else:
        print("❌ Some core functionality tests FAILED!")
        print("⚠️  Please review and fix issues before deployment.")
        return 1

if __name__ == "__main__":
    sys.exit(main())