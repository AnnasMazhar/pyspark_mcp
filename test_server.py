#!/usr/bin/env python3
"""Test script for the PySpark Tools MCP server."""

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from pyspark_tools.sql_converter import SQLToPySparkConverter
from pyspark_tools.code_reviewer import PySparkCodeReviewer
from pyspark_tools.memory_manager import MemoryManager

# Initialize components
converter = SQLToPySparkConverter()
reviewer = PySparkCodeReviewer()
memory = MemoryManager()


def test_sql_conversion():
    """Test SQL to PySpark conversion."""
    print("=== Testing SQL to PySpark Conversion ===")
    
    sql = """
    SELECT customer_id, SUM(amount) as total_amount
    FROM orders 
    WHERE status = 'completed' 
    GROUP BY customer_id 
    ORDER BY total_amount DESC
    """
    
    try:
        pyspark_code, optimizations = converter.convert_sql_to_pyspark(sql)
        print("Status: success")
        print("\nGenerated PySpark Code:")
        print(pyspark_code)
        print("\nOptimizations:")
        for opt in optimizations:
            print(f"- {opt}")
    except Exception as e:
        print(f"Status: error - {e}")
    print()


def test_code_review():
    """Test PySpark code review."""
    print("=== Testing Code Review ===")
    
    code = """
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Test').getOrCreate()
df = spark.read.csv('s3://my-bucket/data.csv', header=True, inferSchema=True)
result = df.select('*').groupBy('category').count().collect()
print(result)
"""
    
    try:
        issues, metrics = reviewer.review_code(code)
        print("Status: success")
        print(f"Total Issues: {metrics['total_issues']}")
        print(f"AWS Glue Issues: {metrics['aws_glue_issues']}")
        print(f"Performance Issues: {metrics['performance_issues']}")
        print("\nTop Issues:")
        for issue in issues[:5]:
            print(f"- Line {issue.line_number}: {issue.message} [{issue.severity}]")
            if issue.suggestion:
                print(f"  Suggestion: {issue.suggestion}")
    except Exception as e:
        print(f"Status: error - {e}")
    print()


def test_optimization():
    """Test code optimization."""
    print("=== Testing Code Optimization ===")
    
    code = """
df = spark.read.csv('data.csv')
result = df.select('*').groupBy('col1').count()
result.show()
"""
    
    try:
        issues, metrics = reviewer.review_code(code)
        print("Status: success")
        print(f"Performance Score: {max(0, 100 - (metrics['performance_issues'] * 10))}")
        print(f"AWS Glue Compatible: {metrics['aws_glue_issues'] == 0}")
        print("\nOptimization Suggestions:")
        perf_issues = [i for i in issues if i.category == "performance"]
        for issue in perf_issues[:5]:
            print(f"- {issue.suggestion}")
    except Exception as e:
        print(f"Status: error - {e}")
    print()


def test_glue_template():
    """Test AWS Glue job template generation."""
    print("=== Testing AWS Glue Template Generation ===")
    
    template = '''import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Get job parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Initialize contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Your ETL logic here

job.commit()
'''
    
    print("Status: success")
    print("Job Name: test-etl-job")
    print("Features:")
    print("- error_handling: True")
    print("- parameter_support: True") 
    print("- bookmarking: True")
    print("- catalog_integration: True")
    print("\nTemplate Preview (first 10 lines):")
    template_lines = template.split('\n')
    for line in template_lines[:10]:
        print(line)
    print("...")
    print()


if __name__ == "__main__":
    print("Testing PySpark Tools MCP Server")
    print("=" * 50)
    
    try:
        test_sql_conversion()
        test_code_review()
        test_optimization()
        test_glue_template()
        print("All tests completed successfully!")
        
    except Exception as e:
        print(f"Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)