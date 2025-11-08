#!/usr/bin/env python3
"""
Comprehensive MCP Server Validation Script

This script validates all aspects of the PySpark Tools MCP Server
to ensure it's ready for production use.
"""

import json
import sys
import tempfile
import time
from pathlib import Path
from typing import Dict, List, Any

# Add the project root to the path
sys.path.insert(0, str(Path(__file__).parent.parent))

from pyspark_tools.server import app
from pyspark_tools.sql_converter import SQLToPySparkConverter
from pyspark_tools.memory_manager import MemoryManager
from pyspark_tools.batch_processor import BatchProcessor
from pyspark_tools.aws_glue_integration import AWSGlueIntegration


class MCPServerValidator:
    """Comprehensive MCP server validation."""
    
    def __init__(self):
        self.results = {}
        self.memory_manager = MemoryManager(":memory:")
        
    def validate_server_initialization(self) -> Dict:
        """Validate that the MCP server initializes correctly."""
        print("🚀 Validating MCP server initialization...")
        
        try:
            # Check if server app exists and has tools
            if not hasattr(app, 'tools'):
                return {'passed': False, 'error': 'Server app has no tools attribute'}
            
            tools = list(app.tools.keys())
            expected_tools = [
                'convert_sql_to_pyspark',
                'review_pyspark_code',
                'optimize_pyspark_code',
                'generate_aws_glue_job_template',
                'batch_process_files',
                'analyze_code_patterns'
            ]
            
            missing_tools = [tool for tool in expected_tools if tool not in tools]
            
            result = {
                'total_tools': len(tools),
                'expected_tools': len(expected_tools),
                'missing_tools': missing_tools,
                'available_tools': tools,
                'passed': len(missing_tools) == 0
            }
            
            print(f"   Total tools registered: {len(tools)}")
            print(f"   Expected core tools: {len(expected_tools)}")
            print(f"   Missing tools: {len(missing_tools)}")
            if missing_tools:
                print(f"   Missing: {missing_tools}")
            print(f"   Status: {'✅ PASSED' if result['passed'] else '❌ FAILED'}")
            
            return result
            
        except Exception as e:
            return {'passed': False, 'error': str(e)}
    
    def validate_sql_conversion_tools(self) -> Dict:
        """Validate SQL conversion MCP tools."""
        print("🔄 Validating SQL conversion tools...")
        
        test_cases = [
            {
                'name': 'Simple SELECT',
                'sql': 'SELECT id, name FROM users WHERE age > 25',
                'dialect': 'postgres'
            },
            {
                'name': 'Complex JOIN with aggregation',
                'sql': '''
                SELECT u.name, COUNT(o.id) as order_count
                FROM users u
                LEFT JOIN orders o ON u.id = o.user_id
                WHERE u.created_at > '2023-01-01'
                GROUP BY u.name
                ORDER BY order_count DESC
                ''',
                'dialect': 'postgres'
            },
            {
                'name': 'Window function',
                'sql': '''
                SELECT name, salary,
                       ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as rank
                FROM employees
                ''',
                'dialect': 'spark'
            }
        ]
        
        results = []
        converter = SQLToPySparkConverter()
        
        for test_case in test_cases:
            try:
                start_time = time.time()
                result = converter.convert_sql_to_pyspark(
                    test_case['sql'], 
                    dialect=test_case['dialect']
                )
                conversion_time = time.time() - start_time
                
                test_result = {
                    'name': test_case['name'],
                    'success': bool(result.pyspark_code),
                    'conversion_time': conversion_time,
                    'has_optimizations': len(result.optimizations) > 0,
                    'dialect_detected': result.dialect_used == test_case['dialect']
                }
                
                results.append(test_result)
                
                status = "✅" if test_result['success'] else "❌"
                print(f"   {status} {test_case['name']}: {conversion_time:.3f}s")
                
            except Exception as e:
                results.append({
                    'name': test_case['name'],
                    'success': False,
                    'error': str(e)
                })
                print(f"   ❌ {test_case['name']}: FAILED - {e}")
        
        successful = sum(1 for r in results if r.get('success', False))
        total = len(results)
        
        overall_result = {
            'test_cases': results,
            'successful': successful,
            'total': total,
            'success_rate': (successful / total) * 100,
            'passed': successful >= total * 0.8  # 80% success rate required
        }
        
        print(f"   Success rate: {overall_result['success_rate']:.1f}% ({successful}/{total})")
        print(f"   Status: {'✅ PASSED' if overall_result['passed'] else '❌ FAILED'}")
        
        return overall_result
    
    def validate_aws_glue_tools(self) -> Dict:
        """Validate AWS Glue integration tools."""
        print("☁️  Validating AWS Glue tools...")
        
        try:
            glue_integration = AWSGlueIntegration()
            
            # Test job template generation
            template_result = glue_integration.generate_glue_job_template(
                job_name="test_job",
                source_format="parquet",
                target_format="parquet"
            )
            
            # Test DynamicFrame conversion
            test_code = """
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.getOrCreate()
            df = spark.read.parquet('s3://bucket/data/')
            df.write.parquet('s3://bucket/output/')
            """
            
            conversion_result = glue_integration.convert_dataframe_to_dynamic_frame(
                pyspark_code=test_code,
                source_database="test_db",
                source_table="test_table",
                target_database="output_db",
                target_table="output_table"
            )
            
            result = {
                'template_generation': template_result['status'] == 'success',
                'dynamic_frame_conversion': conversion_result['status'] == 'success',
                'template_has_code': 'job_template' in template_result,
                'conversion_has_code': 'dynamic_frame_code' in conversion_result,
                'passed': (template_result['status'] == 'success' and 
                          conversion_result['status'] == 'success')
            }
            
            print(f"   Template generation: {'✅' if result['template_generation'] else '❌'}")
            print(f"   DynamicFrame conversion: {'✅' if result['dynamic_frame_conversion'] else '❌'}")
            print(f"   Status: {'✅ PASSED' if result['passed'] else '❌ FAILED'}")
            
            return result
            
        except Exception as e:
            return {'passed': False, 'error': str(e)}
    
    def validate_batch_processing_tools(self) -> Dict:
        """Validate batch processing tools."""
        print("📦 Validating batch processing tools...")
        
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_path = Path(temp_dir)
                
                # Create test SQL files
                test_files = []
                for i in range(5):
                    file_path = temp_path / f"test_{i}.sql"
                    with open(file_path, 'w') as f:
                        f.write(f"SELECT id, name FROM table_{i} WHERE active = true;")
                    test_files.append(str(file_path))
                
                # Test batch processing
                processor = BatchProcessor(self.memory_manager)
                result = processor.process_files(test_files, str(temp_path / "output"))
                
                validation_result = {
                    'files_processed': result['total_files'],
                    'successful_conversions': result['successful_conversions'],
                    'failed_conversions': result['failed_conversions'],
                    'success_rate': (result['successful_conversions'] / result['total_files']) * 100,
                    'has_summary': 'summary' in result,
                    'passed': result['successful_conversions'] >= 4  # At least 4/5 should succeed
                }
                
                print(f"   Files processed: {validation_result['files_processed']}")
                print(f"   Successful: {validation_result['successful_conversions']}")
                print(f"   Success rate: {validation_result['success_rate']:.1f}%")
                print(f"   Status: {'✅ PASSED' if validation_result['passed'] else '❌ FAILED'}")
                
                return validation_result
                
        except Exception as e:
            return {'passed': False, 'error': str(e)}
    
    def validate_performance_targets(self) -> Dict:
        """Validate performance targets are met."""
        print("⚡ Validating performance targets...")
        
        # Test conversion speed
        test_sql = "SELECT u.name, COUNT(o.id) FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.name"
        converter = SQLToPySparkConverter()
        
        conversion_times = []
        for _ in range(10):  # Run 10 times for average
            start_time = time.time()
            result = converter.convert_sql_to_pyspark(test_sql)
            conversion_time = time.time() - start_time
            conversion_times.append(conversion_time)
        
        avg_time = sum(conversion_times) / len(conversion_times)
        max_time = max(conversion_times)
        
        performance_result = {
            'average_conversion_time': avg_time,
            'max_conversion_time': max_time,
            'target_time': 2.0,
            'speed_target_met': max_time < 2.0,
            'consistency_good': (max_time - min(conversion_times)) < 1.0,
            'passed': max_time < 2.0
        }
        
        print(f"   Average conversion time: {avg_time:.3f}s")
        print(f"   Max conversion time: {max_time:.3f}s (target: <2s)")
        print(f"   Performance consistency: {'✅' if performance_result['consistency_good'] else '❌'}")
        print(f"   Status: {'✅ PASSED' if performance_result['passed'] else '❌ FAILED'}")
        
        return performance_result
    
    def run_comprehensive_validation(self) -> Dict:
        """Run all validation tests."""
        print("🔍 COMPREHENSIVE MCP SERVER VALIDATION")
        print("=" * 60)
        
        validations = [
            ('server_initialization', self.validate_server_initialization),
            ('sql_conversion_tools', self.validate_sql_conversion_tools),
            ('aws_glue_tools', self.validate_aws_glue_tools),
            ('batch_processing_tools', self.validate_batch_processing_tools),
            ('performance_targets', self.validate_performance_targets)
        ]
        
        results = {}
        all_passed = True
        
        for name, validation_func in validations:
            print(f"\n{name.replace('_', ' ').title()}:")
            try:
                result = validation_func()
                results[name] = result
                if not result['passed']:
                    all_passed = False
            except Exception as e:
                print(f"   ❌ FAILED: {e}")
                results[name] = {'passed': False, 'error': str(e)}
                all_passed = False
        
        # Overall summary
        print("\n" + "=" * 60)
        print("📊 VALIDATION SUMMARY")
        print("=" * 60)
        
        for name, result in results.items():
            status = "✅ PASSED" if result['passed'] else "❌ FAILED"
            print(f"{name.replace('_', ' ').title()}: {status}")
        
        print(f"\nOverall Status: {'✅ MCP SERVER READY' if all_passed else '❌ ISSUES FOUND'}")
        
        # Save results
        results_file = Path(__file__).parent.parent / "mcp_validation_results.json"
        with open(results_file, 'w') as f:
            json.dump({
                'timestamp': time.time(),
                'overall_passed': all_passed,
                'validation_results': results
            }, f, indent=2, default=str)
        
        print(f"\n📄 Detailed results saved to: {results_file}")
        
        return {
            'overall_passed': all_passed,
            'results': results
        }


def main():
    """Main entry point for MCP server validation."""
    validator = MCPServerValidator()
    results = validator.run_comprehensive_validation()
    
    # Exit with appropriate code
    sys.exit(0 if results['overall_passed'] else 1)


if __name__ == "__main__":
    main()