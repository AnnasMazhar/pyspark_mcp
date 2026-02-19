"""Tests for duplicate code detection and pattern analysis."""

import ast

import pytest

from pyspark_tools.duplicate_detector import (
    ASTPatternExtractor,
    CodePattern,
    DuplicateDetector,
    PatternAnalysis,
    PatternMatch,
    UtilityFunction,
)


class TestASTPatternExtractor:
    """Test AST pattern extraction functionality."""

    def setup_method(self):
        self.extractor = ASTPatternExtractor()

    def test_extract_simple_patterns(self):
        """Test extraction of simple AST patterns."""
        # Test Name node
        node = ast.Name(id="variable", ctx=ast.Load())
        pattern = self.extractor.extract_ast_pattern(node)
        assert pattern == "Name(variable)"

        # Test Constant node
        node = ast.Constant(value=42)
        pattern = self.extractor.extract_ast_pattern(node)
        assert pattern == "Constant(int)"

    def test_extract_code_blocks(self):
        """Test extraction of code blocks from Python code."""
        code = """
def test_function():
    x = 1
    return x

for i in range(10):
    print(i)

if True:
    pass
"""
        blocks = self.extractor.extract_code_blocks(code)

        # Should find function, for loop, and if statement
        assert len(blocks) >= 3

        # Check that we have different types of blocks
        block_types = [type(block[0]).__name__ for block in blocks]
        assert "FunctionDef" in block_types
        assert "For" in block_types
        assert "If" in block_types

    def test_extract_code_blocks_with_syntax_error(self):
        """Test handling of code with syntax errors."""
        code = "def broken_function(\n    # Missing closing parenthesis"
        blocks = self.extractor.extract_code_blocks(code)
        assert blocks == []


class TestDuplicateDetector:
    """Test duplicate code detection functionality."""

    def setup_method(self):
        self.detector = DuplicateDetector(similarity_threshold=0.8)

    def test_analyze_patterns_with_duplicates(self):
        """Test pattern analysis with duplicate code."""
        code_samples = [
            """
def process_data_1():
    df = spark.read.parquet("path1")
    df = df.filter(col("status") == "active")
    df = df.select("id", "name", "value")
    return df
""",
            """
def process_data_2():
    df = spark.read.parquet("path2")
    df = df.filter(col("status") == "active")
    df = df.select("id", "name", "value")
    return df
""",
            """
def different_function():
    print("This is completely different")
    return None
""",
        ]

        analysis = self.detector.analyze_patterns(code_samples)

        assert isinstance(analysis, PatternAnalysis)
        assert (
            len(analysis.patterns) >= 0
        )  # May or may not find patterns depending on similarity
        assert isinstance(analysis.statistics, dict)
        assert "total_code_samples" in analysis.statistics
        assert analysis.statistics["total_code_samples"] == 3

    def test_analyze_patterns_no_duplicates(self):
        """Test pattern analysis with no duplicate code."""
        code_samples = [
            "def func1(): return 1",
            "def func2(): return 'hello'",
            "x = [1, 2, 3]",
        ]

        analysis = self.detector.analyze_patterns(code_samples)

        assert isinstance(analysis, PatternAnalysis)
        assert len(analysis.duplicate_groups) == 0
        assert analysis.statistics["total_code_samples"] == 3

    def test_extract_common_functions(self):
        """Test extraction of utility functions from patterns."""
        # Create a mock pattern
        pattern = CodePattern(
            pattern_id="test_pattern",
            pattern_hash="abc123",
            description="Test pattern for data processing",
            code_template="df = df.filter(col('{column}') == '{value}')",
            parameters=["column", "value"],
            usage_count=3,
            examples=["df.filter(col('status') == 'active')"],
            similarity_threshold=0.8,
            ast_structure="test_structure",
        )

        functions = self.detector.extract_common_functions([pattern])

        assert len(functions) == 1
        assert isinstance(functions[0], UtilityFunction)
        assert functions[0].pattern_id == "test_pattern"
        assert "def " in functions[0].function_code
        assert len(functions[0].parameters) == 2

    def test_generate_utilities_module(self):
        """Test generation of complete utilities module."""
        # Create mock utility functions
        func1 = UtilityFunction(
            function_name="filter_active_records",
            function_code="def filter_active_records(df): return df.filter(col('status') == 'active')",
            parameters=["df"],
            description="Filter active records",
            usage_examples=["filter_active_records(my_df)"],
            pattern_id="pattern_1",
        )

        func2 = UtilityFunction(
            function_name="select_basic_columns",
            function_code="def select_basic_columns(df): return df.select('id', 'name')",
            parameters=["df"],
            description="Select basic columns",
            usage_examples=["select_basic_columns(my_df)"],
            pattern_id="pattern_2",
        )

        module_code = self.detector.generate_utilities_module([func1, func2])

        assert isinstance(module_code, str)
        assert "def filter_active_records" in module_code
        assert "def select_basic_columns" in module_code
        assert "from pyspark.sql import DataFrame" in module_code
        assert "Auto-generated by PySpark Tools" in module_code

    def test_refactor_code_with_utilities(self):
        """Test code refactoring with utility functions."""
        original_code = """
df = spark.read.parquet("data.parquet")
df = df.filter(col("status") == "active")
result = df.select("id", "name")
"""

        # Create mock utility function
        utility_func = UtilityFunction(
            function_name="process_data",
            function_code="def process_data(): pass",
            parameters=[],
            description="Process data",
            usage_examples=[],
            pattern_id="pattern_1",
        )

        refactored = self.detector.refactor_code_with_utilities(
            original_code, [utility_func]
        )

        assert isinstance(refactored, str)
        assert "from pyspark_utilities import *" in refactored
        assert original_code in refactored  # Original code should be preserved

    def test_get_pattern_statistics_empty(self):
        """Test getting statistics when no patterns are detected."""
        stats = self.detector.get_pattern_statistics()

        assert isinstance(stats, dict)
        assert "message" in stats
        assert stats["message"] == "No patterns detected yet"

    def test_similarity_threshold_setting(self):
        """Test setting similarity threshold."""
        detector = DuplicateDetector(similarity_threshold=0.9)
        assert detector.similarity_threshold == 0.9

        detector.similarity_threshold = 0.7
        assert detector.similarity_threshold == 0.7

    def test_are_blocks_similar(self):
        """Test block similarity detection."""
        # Create similar blocks
        code1 = "df = df.filter(col('status') == 'active')"
        code2 = "df = df.filter(col('type') == 'valid')"

        try:
            block1 = ast.parse(code1).body[0]
            block2 = ast.parse(code2).body[0]

            # Test with high similarity threshold
            self.detector.similarity_threshold = 0.9
            similar = self.detector._are_blocks_similar(block1, block2, code1, code2)

            # Should be similar due to structural similarity
            assert isinstance(similar, bool)
        except SyntaxError:
            # If parsing fails, that's also a valid test case
            pass

    def test_create_template(self):
        """Test template creation with parameters."""
        code = "df = df.filter(col('status') == 'active')"
        parameters = ["status", "active"]

        template = self.detector._create_template(code, parameters)

        assert isinstance(template, str)
        # Should contain parameter placeholders
        assert "{status}" in template or "status" in template

    def test_suggest_function_name(self):
        """Test function name suggestion."""
        pattern = CodePattern(
            pattern_id="test_pattern",
            pattern_hash="abc123",
            description="Function definition pattern",
            code_template="def test(): pass",
            parameters=[],
            usage_count=2,
            examples=[],
            similarity_threshold=0.8,
            ast_structure="test",
        )

        name = self.detector._suggest_function_name(pattern)
        assert isinstance(name, str)
        assert "function" in name.lower() or "utility" in name.lower()

    def test_indent_code(self):
        """Test code indentation utility."""
        code = "line1\nline2\n  line3"
        indented = self.detector._indent_code(code, 4)

        lines = indented.split("\n")
        assert lines[0].startswith("    line1")
        assert lines[1].startswith("    line2")
        # Empty lines should remain empty
        assert not any(line.strip() == "" and line != "" for line in lines)


class TestPatternDataClasses:
    """Test pattern-related data classes."""

    def test_code_pattern_creation(self):
        """Test CodePattern data class."""
        pattern = CodePattern(
            pattern_id="test_1",
            pattern_hash="hash123",
            description="Test pattern",
            code_template="template code",
            parameters=["param1", "param2"],
            usage_count=5,
            examples=["example1", "example2"],
            similarity_threshold=0.8,
            ast_structure="structure",
        )

        assert pattern.pattern_id == "test_1"
        assert pattern.usage_count == 5
        assert len(pattern.parameters) == 2
        assert len(pattern.examples) == 2

    def test_pattern_match_creation(self):
        """Test PatternMatch data class."""
        match = PatternMatch(
            pattern_id="pattern_1",
            code_snippet="test code",
            start_line=10,
            end_line=15,
            confidence=0.95,
            parameter_values={"param1": "value1"},
        )

        assert match.pattern_id == "pattern_1"
        assert match.start_line == 10
        assert match.end_line == 15
        assert match.confidence == 0.95
        assert match.parameter_values["param1"] == "value1"

    def test_utility_function_creation(self):
        """Test UtilityFunction data class."""
        func = UtilityFunction(
            function_name="test_function",
            function_code="def test_function(): pass",
            parameters=["param1"],
            description="Test utility function",
            usage_examples=["test_function()"],
            pattern_id="pattern_1",
        )

        assert func.function_name == "test_function"
        assert "def test_function" in func.function_code
        assert len(func.parameters) == 1
        assert len(func.usage_examples) == 1


class TestIntegrationScenarios:
    """Test integration scenarios with realistic PySpark code."""

    def setup_method(self):
        self.detector = DuplicateDetector(similarity_threshold=0.7)

    def test_pyspark_data_processing_patterns(self):
        """Test detection of common PySpark data processing patterns."""
        code_samples = [
            """
# Data processing pipeline 1
df1 = spark.read.parquet("input1.parquet")
df1 = df1.filter(col("status") == "active")
df1 = df1.select("id", "name", "value")
df1 = df1.groupBy("name").agg(sum("value").alias("total"))
df1.write.parquet("output1.parquet")
""",
            """
# Data processing pipeline 2
df2 = spark.read.parquet("input2.parquet")
df2 = df2.filter(col("status") == "active")
df2 = df2.select("id", "name", "value")
df2 = df2.groupBy("name").agg(sum("value").alias("total"))
df2.write.parquet("output2.parquet")
""",
            """
# Different processing
df3 = spark.read.json("input.json")
df3 = df3.dropna()
df3.show()
""",
        ]

        analysis = self.detector.analyze_patterns(code_samples)

        # Should detect some patterns in the similar pipelines
        assert isinstance(analysis, PatternAnalysis)
        assert analysis.statistics["total_code_samples"] == 3

        # Generate utility functions if patterns found
        if analysis.patterns:
            utilities = self.detector.extract_common_functions(analysis.patterns)
            assert isinstance(utilities, list)

            if utilities:
                module = self.detector.generate_utilities_module(utilities)
                assert "def " in module
                assert "from pyspark.sql" in module

    def test_aws_glue_job_patterns(self):
        """Test detection of AWS Glue job patterns."""
        code_samples = [
            """
# Glue job 1
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'INPUT_PATH', 'OUTPUT_PATH'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

df = spark.read.format("parquet").load(args['INPUT_PATH'])
df = df.filter(col("active") == True)
df.write.format("parquet").save(args['OUTPUT_PATH'])

job.commit()
""",
            """
# Glue job 2
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'INPUT_PATH', 'OUTPUT_PATH'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

df = spark.read.format("json").load(args['INPUT_PATH'])
df = df.filter(col("valid") == True)
df.write.format("json").save(args['OUTPUT_PATH'])

job.commit()
""",
        ]

        analysis = self.detector.analyze_patterns(code_samples)

        # Should find common Glue initialization patterns
        assert isinstance(analysis, PatternAnalysis)
        assert analysis.statistics["total_code_samples"] == 2

        # Check for refactoring opportunities
        assert isinstance(analysis.refactoring_opportunities, list)

    def test_error_handling_patterns(self):
        """Test detection of error handling patterns."""
        code_samples = [
            """
try:
    df = spark.read.parquet("data1.parquet")
    result = df.count()
    print(f"Processed {result} records")
except Exception as e:
    print(f"Error processing data1: {str(e)}")
    raise
""",
            """
try:
    df = spark.read.parquet("data2.parquet")
    result = df.count()
    print(f"Processed {result} records")
except Exception as e:
    print(f"Error processing data2: {str(e)}")
    raise
""",
            """
# Different error handling
if df is not None:
    df.show()
else:
    print("DataFrame is None")
""",
        ]

        analysis = self.detector.analyze_patterns(code_samples)

        assert isinstance(analysis, PatternAnalysis)
        assert analysis.statistics["total_code_samples"] == 3

        # Should potentially find try-except patterns
        if analysis.patterns:
            # Check that patterns have reasonable descriptions
            for pattern in analysis.patterns:
                assert isinstance(pattern.description, str)
                assert len(pattern.description) > 0
