"""Tests for AWS Glue integration module."""

from unittest.mock import Mock, patch

import pytest

from pyspark_tools.aws_glue_integration import (
    AWSGlueIntegration,
    DataCatalogTable,
    DataFormat,
    GlueJobConfig,
    GlueJobType,
)


class TestAWSGlueIntegration:
    """Test cases for AWS Glue integration."""

    def setup_method(self):
        """Set up test fixtures."""
        self.glue_integration = AWSGlueIntegration()

    def test_initialization(self):
        """Test AWS Glue integration initialization."""
        assert self.glue_integration is not None
        assert len(self.glue_integration.supported_formats) == 6
        assert DataFormat.PARQUET in self.glue_integration.supported_formats

    def test_glue_job_config_creation(self):
        """Test GlueJobConfig creation with defaults."""
        config = GlueJobConfig(job_name="test-job")
        
        assert config.job_name == "test-job"
        assert config.job_type == GlueJobType.ETL
        assert config.source_format == DataFormat.PARQUET
        assert config.target_format == DataFormat.PARQUET
        assert config.include_bookmarking is True
        assert config.use_dynamic_frame is True
        assert config.glue_version == "4.0"

    def test_data_catalog_table_creation(self):
        """Test DataCatalogTable creation."""
        table = DataCatalogTable(
            database_name="test_db",
            table_name="test_table",
            format=DataFormat.PARQUET,
            partition_keys=["year", "month"],
        )
        
        assert table.database_name == "test_db"
        assert table.table_name == "test_table"
        assert table.format == DataFormat.PARQUET
        assert table.partition_keys == ["year", "month"]

    def test_generate_enhanced_glue_job_template_basic(self):
        """Test basic enhanced Glue job template generation."""
        config = GlueJobConfig(job_name="test-etl-job")
        
        result = self.glue_integration.generate_enhanced_glue_job_template(config)
        
        assert result["status"] == "success"
        assert result["job_name"] == "test-etl-job"
        assert result["job_type"] == "etl"
        assert "template" in result
        assert "features" in result
        assert result["features"]["dynamic_frame_support"] is True
        assert result["features"]["bookmarking"] is True

    def test_generate_enhanced_glue_job_template_with_tables(self):
        """Test enhanced Glue job template generation with Data Catalog tables."""
        config = GlueJobConfig(
            job_name="test-catalog-job",
            use_dynamic_frame=True,
        )
        
        source_table = DataCatalogTable(
            database_name="source_db",
            table_name="source_table",
        )
        
        target_table = DataCatalogTable(
            database_name="target_db",
            table_name="target_table",
        )
        
        result = self.glue_integration.generate_enhanced_glue_job_template(
            config=config,
            source_table=source_table,
            target_table=target_table,
        )
        
        assert result["status"] == "success"
        template = result["template"]
        assert "create_dynamic_frame.from_catalog" in template
        assert "source_db" in template
        assert "source_table" in template
        assert "target_db" in template
        assert "target_table" in template

    def test_generate_enhanced_glue_job_template_with_sql(self):
        """Test enhanced Glue job template generation with SQL transformation."""
        config = GlueJobConfig(job_name="test-sql-job")
        
        sql_query = """
        SELECT customer_id, SUM(amount) as total_amount
        FROM source_table
        WHERE status = 'active'
        GROUP BY customer_id
        """
        
        result = self.glue_integration.generate_enhanced_glue_job_template(
            config=config,
            transformation_sql=sql_query,
        )
        
        assert result["status"] == "success"
        template = result["template"]
        assert "spark.sql" in template
        assert "SELECT customer_id" in template
        assert "GROUP BY customer_id" in template

    def test_generate_imports_basic(self):
        """Test import generation for basic configuration."""
        config = GlueJobConfig(job_name="test-job", use_dynamic_frame=False)
        
        imports = self.glue_integration._generate_imports(config)
        
        assert "import sys" in imports
        assert "from awsglue.transforms import *" in imports
        assert "from pyspark.context import SparkContext" in imports
        assert "from awsglue.context import GlueContext" in imports
        assert "from awsglue.job import Job" in imports

    def test_generate_imports_with_dynamic_frame(self):
        """Test import generation with DynamicFrame support."""
        config = GlueJobConfig(job_name="test-job", use_dynamic_frame=True)
        
        imports = self.glue_integration._generate_imports(config)
        
        assert "from awsglue.dynamicframe import DynamicFrame" in imports
        assert "from awsglue import DynamicFrame" in imports

    def test_generate_imports_with_logging(self):
        """Test import generation with continuous logging."""
        config = GlueJobConfig(
            job_name="test-job",
            enable_continuous_logging=True,
        )
        
        imports = self.glue_integration._generate_imports(config)
        
        assert "import logging" in imports

    def test_generate_job_initialization_basic(self):
        """Test job initialization code generation."""
        config = GlueJobConfig(job_name="test-job", use_dynamic_frame=False)
        
        init_code = self.glue_integration._generate_job_initialization(config)
        
        assert "getResolvedOptions" in init_code
        assert "'JOB_NAME'" in init_code
        assert "'SOURCE_PATH'" in init_code
        assert "'TARGET_PATH'" in init_code
        assert "SparkContext()" in init_code
        assert "GlueContext(sc)" in init_code

    def test_generate_job_initialization_with_dynamic_frame(self):
        """Test job initialization with DynamicFrame parameters."""
        config = GlueJobConfig(job_name="test-job", use_dynamic_frame=True)
        
        init_code = self.glue_integration._generate_job_initialization(config)
        
        assert "'SOURCE_DATABASE'" in init_code
        assert "'SOURCE_TABLE'" in init_code
        assert "'TARGET_DATABASE'" in init_code
        assert "'TARGET_TABLE'" in init_code

    def test_generate_job_initialization_with_bookmarking(self):
        """Test job initialization with bookmarking."""
        config = GlueJobConfig(job_name="test-job", include_bookmarking=True)
        
        init_code = self.glue_integration._generate_job_initialization(config)
        
        assert "'JOB_BOOKMARK_OPTION'" in init_code

    def test_generate_data_reading_logic_dataframe(self):
        """Test data reading logic for DataFrame approach."""
        config = GlueJobConfig(
            job_name="test-job",
            use_dynamic_frame=False,
            source_format=DataFormat.CSV,
        )
        
        reading_code = self.glue_integration._generate_data_reading_logic(config, None)
        
        assert "spark.read.format" in reading_code
        assert "csv" in reading_code
        assert 'option("header", "true")' in reading_code

    def test_generate_data_reading_logic_dynamic_frame(self):
        """Test data reading logic for DynamicFrame approach."""
        config = GlueJobConfig(job_name="test-job", use_dynamic_frame=True)
        
        source_table = DataCatalogTable(
            database_name="test_db",
            table_name="test_table",
        )
        
        reading_code = self.glue_integration._generate_data_reading_logic(
            config, source_table
        )
        
        assert "create_dynamic_frame.from_catalog" in reading_code
        assert "test_db" in reading_code
        assert "test_table" in reading_code
        assert "toDF()" in reading_code

    def test_generate_transformation_logic_no_sql(self):
        """Test transformation logic generation without SQL."""
        config = GlueJobConfig(job_name="test-job")
        
        transform_code = self.glue_integration._generate_transformation_logic(
            config, None
        )
        
        assert "Apply transformations here" in transform_code
        assert "transformed_df = source_df" in transform_code

    def test_generate_transformation_logic_with_sql(self):
        """Test transformation logic generation with SQL."""
        config = GlueJobConfig(job_name="test-job")
        
        sql_query = "SELECT * FROM source_table WHERE active = true"
        
        transform_code = self.glue_integration._generate_transformation_logic(
            config, sql_query
        )
        
        assert "spark.sql" in transform_code
        assert "createOrReplaceTempView" in transform_code
        assert "SELECT * FROM source_table" in transform_code

    def test_generate_data_writing_logic_dataframe(self):
        """Test data writing logic for DataFrame approach."""
        config = GlueJobConfig(
            job_name="test-job",
            use_dynamic_frame=False,
            target_format=DataFormat.PARQUET,
        )
        
        writing_code = self.glue_integration._generate_data_writing_logic(config, None)
        
        assert "transformed_df.write" in writing_code
        assert "format(\"parquet\")" in writing_code
        assert "mode(\"overwrite\")" in writing_code

    def test_generate_data_writing_logic_dynamic_frame(self):
        """Test data writing logic for DynamicFrame approach."""
        config = GlueJobConfig(job_name="test-job", use_dynamic_frame=True)
        
        target_table = DataCatalogTable(
            database_name="target_db",
            table_name="target_table",
        )
        
        writing_code = self.glue_integration._generate_data_writing_logic(
            config, target_table
        )
        
        assert "DynamicFrame.fromDF" in writing_code
        assert "write_dynamic_frame.from_catalog" in writing_code
        assert "target_db" in writing_code
        assert "target_table" in writing_code

    def test_generate_error_handling_basic(self):
        """Test error handling code generation."""
        config = GlueJobConfig(job_name="test-job", include_bookmarking=False)
        
        error_code = self.glue_integration._generate_error_handling(config)
        
        assert "except Exception as e:" in error_code
        assert "Job failed with error" in error_code
        assert "raise e" in error_code
        assert "finally:" in error_code
        assert "Job bookmarking disabled" in error_code

    def test_generate_error_handling_with_bookmarking(self):
        """Test error handling with bookmarking enabled."""
        config = GlueJobConfig(job_name="test-job", include_bookmarking=True)
        
        error_code = self.glue_integration._generate_error_handling(config)
        
        assert "job.commit()" in error_code

    def test_generate_error_handling_with_logging(self):
        """Test error handling with continuous logging."""
        config = GlueJobConfig(
            job_name="test-job",
            enable_continuous_logging=True,
        )
        
        error_code = self.glue_integration._generate_error_handling(config)
        
        assert "logger.error" in error_code

    def test_get_job_parameters_basic(self):
        """Test job parameters generation."""
        config = GlueJobConfig(job_name="test-job", use_dynamic_frame=False)
        
        params = self.glue_integration._get_job_parameters(config, None, None)
        
        assert params["JOB_NAME"] == "test-job"
        assert "SOURCE_PATH" in params
        assert "TARGET_PATH" in params

    def test_get_job_parameters_with_tables(self):
        """Test job parameters with Data Catalog tables."""
        config = GlueJobConfig(job_name="test-job", use_dynamic_frame=True)
        
        source_table = DataCatalogTable(
            database_name="source_db",
            table_name="source_table",
        )
        
        target_table = DataCatalogTable(
            database_name="target_db",
            table_name="target_table",
        )
        
        params = self.glue_integration._get_job_parameters(
            config, source_table, target_table
        )
        
        assert params["SOURCE_DATABASE"] == "source_db"
        assert params["SOURCE_TABLE"] == "source_table"
        assert params["TARGET_DATABASE"] == "target_db"
        assert params["TARGET_TABLE"] == "target_table"

    def test_get_job_parameters_with_bookmarking(self):
        """Test job parameters with bookmarking."""
        config = GlueJobConfig(job_name="test-job", include_bookmarking=True)
        
        params = self.glue_integration._get_job_parameters(config, None, None)
        
        assert params["JOB_BOOKMARK_OPTION"] == "job-bookmark-enable"

    def test_get_glue_job_properties(self):
        """Test Glue job properties generation."""
        config = GlueJobConfig(
            job_name="test-job",
            worker_type="G.2X",
            number_of_workers=4,
            max_retries=2,
            timeout=1440,
            glue_version="3.0",
        )
        
        properties = self.glue_integration._get_glue_job_properties(config)
        
        assert properties["Name"] == "test-job"
        assert properties["WorkerType"] == "G.2X"
        assert properties["NumberOfWorkers"] == 4
        assert properties["MaxRetries"] == 2
        assert properties["Timeout"] == 1440
        assert properties["GlueVersion"] == "3.0"
        assert "Command" in properties
        assert "DefaultArguments" in properties

    def test_generate_dynamic_frame_conversion_template(self):
        """Test DynamicFrame conversion template generation."""
        pyspark_code = """
        df = spark.read.parquet("s3://bucket/data/")
        filtered_df = df.filter(col("status") == "active")
        result_df = filtered_df.select("id", "name", "amount")
        """
        
        source_table = DataCatalogTable(
            database_name="source_db",
            table_name="source_table",
        )
        
        target_table = DataCatalogTable(
            database_name="target_db",
            table_name="target_table",
        )
        
        result = self.glue_integration.generate_dynamic_frame_conversion_template(
            pyspark_code, source_table, target_table
        )
        
        assert result["status"] == "success"
        assert "original_code" in result
        assert "dynamic_frame_code" in result
        assert "conversion_notes" in result
        assert "benefits" in result
        
        dynamic_code = result["dynamic_frame_code"]
        assert "create_dynamic_frame.from_catalog" in dynamic_code
        assert "source_db" in dynamic_code
        assert "target_db" in dynamic_code

    def test_generate_glue_job_with_sql_conversion(self):
        """Test Glue job generation with SQL conversion."""
        sql_query = "SELECT customer_id, SUM(amount) FROM orders GROUP BY customer_id"
        
        config = GlueJobConfig(job_name="sql-conversion-job")
        
        source_table = DataCatalogTable(
            database_name="analytics",
            table_name="orders",
        )
        
        target_table = DataCatalogTable(
            database_name="analytics",
            table_name="customer_totals",
        )
        
        result = self.glue_integration.generate_glue_job_with_sql_conversion(
            sql_query, config, source_table, target_table
        )
        
        assert result["status"] == "success"
        assert "sql_integration" in result
        assert result["sql_integration"]["original_sql"] == sql_query
        assert result["sql_integration"]["conversion_method"] == "integrated_sql_transformation"

    def test_data_format_enum(self):
        """Test DataFormat enum values."""
        assert DataFormat.PARQUET.value == "parquet"
        assert DataFormat.CSV.value == "csv"
        assert DataFormat.JSON.value == "json"
        assert DataFormat.AVRO.value == "avro"
        assert DataFormat.ORC.value == "orc"
        assert DataFormat.DELTA.value == "delta"

    def test_glue_job_type_enum(self):
        """Test GlueJobType enum values."""
        assert GlueJobType.ETL.value == "etl"
        assert GlueJobType.STREAMING.value == "streaming"
        assert GlueJobType.PYTHON_SHELL.value == "pythonshell"

    def test_supported_formats_configuration(self):
        """Test supported formats configuration."""
        formats = self.glue_integration.supported_formats
        
        # Test Parquet configuration
        parquet_config = formats[DataFormat.PARQUET]
        assert "compression" in parquet_config["write_options"]
        assert parquet_config["write_options"]["compression"] == "snappy"
        
        # Test CSV configuration
        csv_config = formats[DataFormat.CSV]
        assert "header" in csv_config["read_options"]
        assert csv_config["read_options"]["header"] == "true"
        
        # Test JSON configuration
        json_config = formats[DataFormat.JSON]
        assert "multiLine" in json_config["read_options"]
        assert json_config["read_options"]["multiLine"] == "true"

    def test_error_handling_in_template_generation(self):
        """Test error handling in template generation."""
        # Test with invalid configuration
        with patch.object(
            self.glue_integration, '_generate_imports', side_effect=Exception("Test error")
        ):
            config = GlueJobConfig(job_name="error-test")
            result = self.glue_integration.generate_enhanced_glue_job_template(config)
            
            assert result["status"] == "error"
            assert "Test error" in result["message"]

    def test_extract_dataframe_operations(self):
        """Test DataFrame operations extraction."""
        pyspark_code = """
        df = spark.read.parquet("s3://bucket/data/")
        filtered_df = df.filter(col("status") == "active")
        selected_df = df.select("id", "name")
        grouped_df = df.groupBy("category").count()
        joined_df = df.join(other_df, "id")
        """
        
        operations = self.glue_integration._extract_dataframe_operations(pyspark_code)
        
        assert len(operations) == 4  # filter, select, groupBy, join
        assert any(".filter(" in op for op in operations)
        assert any(".select(" in op for op in operations)
        assert any(".groupBy(" in op for op in operations)
        assert any(".join(" in op for op in operations)

    def test_convert_to_dynamic_frame_operations(self):
        """Test conversion to DynamicFrame operations."""
        operations = [
            'filtered_df = df.filter(col("status") == "active")',
            'selected_df = df.select("id", "name")',
        ]
        
        source_table = DataCatalogTable(
            database_name="test_db",
            table_name="test_table",
        )
        
        target_table = DataCatalogTable(
            database_name="output_db",
            table_name="output_table",
        )
        
        dynamic_code = self.glue_integration._convert_to_dynamic_frame_operations(
            operations, source_table, target_table
        )
        
        assert "create_dynamic_frame.from_catalog" in dynamic_code
        assert "test_db" in dynamic_code
        assert "test_table" in dynamic_code
        assert "output_db" in dynamic_code
        assert "output_table" in dynamic_code
        assert "Filter.apply" in dynamic_code
        assert "SelectFields.apply" in dynamic_code

    def test_generate_data_catalog_table_definition(self):
        """Test Data Catalog table definition generation."""
        from pyspark_tools.aws_glue_integration import ColumnInfo, PartitionInfo
        
        columns = [
            ColumnInfo(name="id", type="bigint", comment="Primary key"),
            ColumnInfo(name="name", type="string", comment="Customer name"),
            ColumnInfo(name="amount", type="double", comment="Transaction amount"),
        ]
        
        partitions = [
            PartitionInfo(key="year", type="string", comment="Year partition"),
            PartitionInfo(key="month", type="string", comment="Month partition"),
        ]
        
        table = DataCatalogTable(
            database_name="analytics",
            table_name="transactions",
            s3_location="s3://my-bucket/data/transactions/",
            format=DataFormat.PARQUET,
            columns=columns,
            partitions=partitions,
        )
        
        result = self.glue_integration.generate_data_catalog_table_definition(table)
        
        assert result["status"] == "success"
        assert "table_definition" in result
        assert "aws_cli_command" in result
        assert "terraform_resource" in result
        assert "schema_info" in result
        
        table_def = result["table_definition"]
        assert table_def["Name"] == "transactions"
        assert len(table_def["StorageDescriptor"]["Columns"]) == 3
        assert len(table_def["PartitionKeys"]) == 2
        
        schema_info = result["schema_info"]
        assert schema_info["column_count"] == 3
        assert schema_info["partition_count"] == 2
        assert schema_info["data_format"] == "parquet"

    def test_detect_schema_from_sample_data_single_record(self):
        """Test schema detection from single record."""
        sample_data = {
            "customer_id": 12345,
            "customer_name": "John Doe",
            "order_amount": 99.99,
            "order_date": "2023-01-15",
            "is_premium": True,
            "year": "2023",
            "month": "01",
        }
        
        result = self.glue_integration.detect_schema_from_sample_data(
            sample_data=sample_data,
            table_name="orders",
            infer_partitions=True,
        )
        
        assert result["status"] == "success"
        assert "detected_schema" in result
        assert "schema_statistics" in result
        assert "recommendations" in result
        
        schema = result["detected_schema"]
        assert "columns" in schema
        assert "partitions" in schema
        
        # Check that year and month were detected as partitions
        partition_keys = [p["key"] for p in schema["partitions"]]
        assert "year" in partition_keys
        assert "month" in partition_keys

    def test_detect_schema_from_sample_data_multiple_records(self):
        """Test schema detection from multiple records."""
        sample_data = [
            {"id": 1, "name": "Alice", "score": 95.5, "active": True, "region": "US"},
            {"id": 2, "name": "Bob", "score": 87.2, "active": False, "region": "US"},
            {"id": 3, "name": "Charlie", "score": 92.1, "active": True, "region": "EU"},
        ]
        
        result = self.glue_integration.detect_schema_from_sample_data(
            sample_data=sample_data,
            table_name="users",
            infer_partitions=True,
        )
        
        assert result["status"] == "success"
        
        schema_stats = result["schema_statistics"]
        assert schema_stats["sample_records_analyzed"] == 3
        assert schema_stats["total_fields"] == 5
        
        # Region should be detected as partition due to low cardinality
        schema = result["detected_schema"]
        partition_keys = [p["key"] for p in schema["partitions"]]
        assert "region" in partition_keys

    def test_generate_schema_evolution_strategy_add_columns(self):
        """Test schema evolution strategy for adding columns."""
        from pyspark_tools.aws_glue_integration import ColumnInfo, SchemaEvolutionConfig
        
        current_schema = [
            ColumnInfo(name="id", type="bigint"),
            ColumnInfo(name="name", type="string"),
        ]
        
        new_schema = [
            ColumnInfo(name="id", type="bigint"),
            ColumnInfo(name="name", type="string"),
            ColumnInfo(name="email", type="string"),
            ColumnInfo(name="created_at", type="timestamp"),
        ]
        
        evolution_config = SchemaEvolutionConfig(
            enable_schema_evolution=True,
            merge_behavior="merge",
        )
        
        result = self.glue_integration.generate_schema_evolution_strategy(
            current_schema=current_schema,
            new_schema=new_schema,
            evolution_config=evolution_config,
        )
        
        assert result["status"] == "success"
        assert result["evolution_strategy"]["changes_detected"] is True
        assert result["evolution_strategy"]["backward_compatible"] is True
        assert result["schema_changes"]["added_columns"] == 2
        assert result["schema_changes"]["removed_columns"] == 0
        assert result["schema_changes"]["modified_columns"] == 0

    def test_generate_schema_evolution_strategy_modify_columns(self):
        """Test schema evolution strategy for modifying columns."""
        from pyspark_tools.aws_glue_integration import ColumnInfo, SchemaEvolutionConfig
        
        current_schema = [
            ColumnInfo(name="id", type="int"),
            ColumnInfo(name="amount", type="float"),
        ]
        
        new_schema = [
            ColumnInfo(name="id", type="bigint"),
            ColumnInfo(name="amount", type="double"),
        ]
        
        evolution_config = SchemaEvolutionConfig(
            enable_schema_evolution=True,
            merge_behavior="merge",
        )
        
        result = self.glue_integration.generate_schema_evolution_strategy(
            current_schema=current_schema,
            new_schema=new_schema,
            evolution_config=evolution_config,
        )
        
        assert result["status"] == "success"
        assert result["evolution_strategy"]["migration_required"] is True
        assert result["schema_changes"]["modified_columns"] == 2
        assert len(result["migration_steps"]) > 0

    def test_infer_data_type(self):
        """Test data type inference."""
        # Test basic types
        assert self.glue_integration._infer_data_type(None) == "string"
        assert self.glue_integration._infer_data_type(True) == "boolean"
        assert self.glue_integration._infer_data_type(42) == "bigint"
        assert self.glue_integration._infer_data_type(3.14) == "double"
        assert self.glue_integration._infer_data_type("hello") == "string"
        
        # Test date/timestamp patterns
        assert self.glue_integration._infer_data_type("2023-01-15") == "date"
        assert self.glue_integration._infer_data_type("2023-01-15T10:30:00") == "timestamp"
        assert self.glue_integration._infer_data_type("2023-01-15 10:30:00") == "timestamp"
        
        # Test collections
        assert self.glue_integration._infer_data_type([1, 2, 3]) == "array<bigint>"
        assert self.glue_integration._infer_data_type(["a", "b"]) == "array<string>"
        assert self.glue_integration._infer_data_type({}) == "struct<>"

    def test_is_likely_partition_key(self):
        """Test partition key detection logic."""
        # Test with date-related field names
        records = [{"year": "2023"}, {"year": "2024"}]
        assert self.glue_integration._is_likely_partition_key("year", records) is True
        assert self.glue_integration._is_likely_partition_key("partition_date", records) is True
        
        # Test with low cardinality
        records = [{"region": "US"} for _ in range(10)] + [{"region": "EU"} for _ in range(5)]
        assert self.glue_integration._is_likely_partition_key("region", records) is True
        
        # Test with high cardinality
        records = [{"user_id": i} for i in range(100)]
        assert self.glue_integration._is_likely_partition_key("user_id", records) is False

    def test_get_serde_configurations(self):
        """Test SerDe configuration generation."""
        # Test Parquet
        assert "ParquetHiveSerDe" in self.glue_integration._get_serde_library(DataFormat.PARQUET)
        assert self.glue_integration._get_serde_parameters(DataFormat.PARQUET) == {}
        
        # Test CSV
        assert "LazySimpleSerDe" in self.glue_integration._get_serde_library(DataFormat.CSV)
        csv_params = self.glue_integration._get_serde_parameters(DataFormat.CSV)
        assert csv_params["field.delim"] == ","
        assert csv_params["skip.header.line.count"] == "1"
        
        # Test JSON
        assert "JsonSerDe" in self.glue_integration._get_serde_library(DataFormat.JSON)
        json_params = self.glue_integration._get_serde_parameters(DataFormat.JSON)
        assert json_params["ignore.malformed.json"] == "true"

    def test_generate_schema_recommendations(self):
        """Test schema optimization recommendations."""
        from pyspark_tools.aws_glue_integration import ColumnInfo, PartitionInfo

        # Test with many columns
        many_columns = [ColumnInfo(name=f"col_{i}", type="string") for i in range(60)]
        recommendations = self.glue_integration._generate_schema_recommendations(
            many_columns, [], []
        )
        assert any("columnar format" in rec for rec in recommendations)
        
        # Test with no partitions but many records
        records = [{"id": i} for i in range(2000)]
        recommendations = self.glue_integration._generate_schema_recommendations(
            [ColumnInfo(name="id", type="bigint")], [], records
        )
        assert any("partition keys" in rec for rec in recommendations)
        
        # Test with mostly string fields
        string_columns = [ColumnInfo(name=f"str_col_{i}", type="string") for i in range(10)]
        recommendations = self.glue_integration._generate_schema_recommendations(
            string_columns, [], []
        )
        assert any("string fields" in rec for rec in recommendations)

    def test_generate_terraform_table_resource(self):
        """Test Terraform resource generation."""
        from pyspark_tools.aws_glue_integration import ColumnInfo, PartitionInfo
        
        table = DataCatalogTable(
            database_name="test-db",
            table_name="test-table",
            s3_location="s3://bucket/path/",
            format=DataFormat.PARQUET,
            columns=[ColumnInfo(name="id", type="bigint", comment="Primary key")],
            partitions=[PartitionInfo(key="year", type="string", comment="Year")],
        )
        
        table_definition = {
            "Name": "test-table",
            "TableType": "EXTERNAL_TABLE",
            "StorageDescriptor": {
                "Columns": [{"Name": "id", "Type": "bigint", "Comment": "Primary key"}],
                "Location": "s3://bucket/path/",
                "InputFormat": "input_format",
                "OutputFormat": "output_format",
                "SerdeInfo": {
                    "SerializationLibrary": "serde_lib",
                    "Parameters": {},
                },
                "Compressed": True,
            },
            "PartitionKeys": [{"Name": "year", "Type": "string", "Comment": "Year"}],
            "Parameters": {},
        }
        
        terraform_code = self.glue_integration._generate_terraform_table_resource(
            table, table_definition
        )
        
        assert 'resource "aws_glue_catalog_table"' in terraform_code
        assert "test-db" in terraform_code
        assert "test-table" in terraform_code
        assert "s3://bucket/path/" in terraform_code
        assert "partition_keys" in terraform_code

    def test_generate_schema_evolution_code(self):
        """Test schema evolution code generation."""
        from pyspark_tools.aws_glue_integration import ColumnInfo, SchemaEvolutionConfig
        
        added_columns = [
            ColumnInfo(name="new_field", type="string"),
            ColumnInfo(name="created_at", type="timestamp"),
        ]
        
        modified_columns = [
            {
                "column": ColumnInfo(name="amount", type="int"),
                "old_type": "int",
                "new_type": "bigint",
            }
        ]
        
        config = SchemaEvolutionConfig()
        
        code = self.glue_integration._generate_schema_evolution_code(
            added_columns, modified_columns, config
        )
        
        assert "Schema evolution transformation" in code
        assert "add_missing_columns" in code
        assert "convert_column_types" in code
        assert "new_field" in code
        assert "created_at" in code
        assert "amount" in code

    def test_get_default_value_for_type(self):
        """Test default value generation for data types."""
        assert self.glue_integration._get_default_value_for_type("string") == '""'
        assert self.glue_integration._get_default_value_for_type("bigint") == "0"
        assert self.glue_integration._get_default_value_for_type("double") == "0.0"
        assert self.glue_integration._get_default_value_for_type("boolean") == "False"
        assert self.glue_integration._get_default_value_for_type("array<string>") == "[]"
        assert self.glue_integration._get_default_value_for_type("struct<>") == "{}"
        assert self.glue_integration._get_default_value_for_type("unknown_type") == "None"

    def test_analyze_s3_optimization_opportunities(self):
        """Test S3 optimization analysis."""
        table_info = DataCatalogTable(
            database_name="test_db",
            table_name="test_table",
            s3_location="s3://test-bucket/data/",
            format=DataFormat.CSV,
        )
        
        query_patterns = [
            "SELECT * FROM table WHERE date >= '2023-01-01'",
            "SELECT region, COUNT(*) FROM table GROUP BY region",
        ]
        
        result = self.glue_integration.analyze_s3_optimization_opportunities(
            s3_location="s3://test-bucket/data/",
            table_info=table_info,
            query_patterns=query_patterns,
            data_size_gb=150.0,
        )
        
        assert result["status"] == "success"
        assert "current_issues" in result
        assert "optimization_recommendations" in result
        assert "estimated_improvements" in result
        assert "implementation_code" in result
        
        # Check that CSV format issue is detected
        assert any("CSV format detected" in issue for issue in result["current_issues"])
        
        # Check that date partitioning is recommended
        assert any("date-based partitioning" in rec for rec in result["optimization_recommendations"])

    def test_generate_s3_optimization_strategy(self):
        """Test S3 optimization strategy generation."""
        from pyspark_tools.aws_glue_integration import S3OptimizationConfig
        
        table_info = DataCatalogTable(
            database_name="analytics",
            table_name="events",
            format=DataFormat.PARQUET,
        )
        
        config = S3OptimizationConfig(
            target_file_size_mb=256,
            compression_type="gzip",
            enable_small_file_optimization=True,
        )
        
        result = self.glue_integration.generate_s3_optimization_strategy(
            table_info=table_info,
            optimization_config=config,
            query_patterns=["SELECT * FROM events WHERE event_date = '2023-01-01'"],
        )
        
        assert result["status"] == "success"
        assert "optimization_strategy" in result
        assert "implementation_code" in result
        assert "glue_job_config" in result
        
        strategy = result["optimization_strategy"]
        assert strategy["file_optimization"]["target_file_size_mb"] == 256
        assert strategy["compression"]["compression_type"] == "gzip"

    def test_generate_small_files_consolidation_job(self):
        """Test small files consolidation job generation."""
        source_table = DataCatalogTable(
            database_name="raw_data",
            table_name="small_files_table",
        )
        
        target_table = DataCatalogTable(
            database_name="optimized_data",
            table_name="consolidated_table",
        )
        
        result = self.glue_integration.generate_small_files_consolidation_job(
            source_table=source_table,
            target_table=target_table,
            target_file_size_mb=128,
            consolidation_strategy="coalesce",
        )
        
        assert result["status"] == "success"
        assert "job_template" in result
        assert "job_parameters" in result
        assert "performance_tips" in result
        
        job_template = result["job_template"]
        assert "coalesce" in job_template
        assert "raw_data" in job_template
        assert "small_files_table" in job_template
        assert "optimized_data" in job_template
        assert "consolidated_table" in job_template

    def test_generate_incremental_processing_job_timestamp(self):
        """Test incremental processing job with timestamp strategy."""
        source_table = DataCatalogTable(
            database_name="source_db",
            table_name="events",
        )
        
        target_table = DataCatalogTable(
            database_name="target_db",
            table_name="processed_events",
        )
        
        result = self.glue_integration.generate_incremental_processing_job(
            source_table=source_table,
            target_table=target_table,
            incremental_column="event_timestamp",
            incremental_strategy="timestamp",
            transformation_sql="SELECT *, UPPER(event_type) as event_type_upper FROM source_table",
        )
        
        assert result["status"] == "success"
        assert result["incremental_strategy"] == "timestamp"
        assert result["incremental_column"] == "event_timestamp"
        assert "job_template" in result
        assert "monitoring_setup" in result
        assert "best_practices" in result
        
        job_template = result["job_template"]
        assert "LOOKBACK_HOURS" in job_template
        assert "INCREMENTAL_COLUMN" in job_template
        assert "UPPER(event_type)" in job_template

    def test_generate_incremental_processing_job_watermark(self):
        """Test incremental processing job with watermark strategy."""
        source_table = DataCatalogTable(
            database_name="source_db",
            table_name="transactions",
        )
        
        target_table = DataCatalogTable(
            database_name="target_db",
            table_name="processed_transactions",
        )
        
        result = self.glue_integration.generate_incremental_processing_job(
            source_table=source_table,
            target_table=target_table,
            incremental_column="transaction_date",
            incremental_strategy="watermark",
        )
        
        assert result["status"] == "success"
        assert result["incremental_strategy"] == "watermark"
        
        job_template = result["job_template"]
        assert "WATERMARK_TABLE" in job_template
        assert "max_value" in job_template
        assert "job_watermarks" in job_template

    def test_generate_incremental_processing_job_bookmark(self):
        """Test incremental processing job with bookmark strategy."""
        source_table = DataCatalogTable(
            database_name="source_db",
            table_name="orders",
        )
        
        target_table = DataCatalogTable(
            database_name="target_db",
            table_name="processed_orders",
        )
        
        result = self.glue_integration.generate_incremental_processing_job(
            source_table=source_table,
            target_table=target_table,
            incremental_column="order_id",
            incremental_strategy="bookmark",
        )
        
        assert result["status"] == "success"
        assert result["incremental_strategy"] == "bookmark"
        
        job_template = result["job_template"]
        assert "source_dynamic_frame_with_bookmark" in job_template
        assert "target_dynamic_frame_with_bookmark" in job_template
        assert "job.commit()" in job_template

    def test_generate_job_bookmark_configuration(self):
        """Test job bookmark configuration generation."""
        result = self.glue_integration.generate_job_bookmark_configuration(
            job_name="test-incremental-job",
            bookmark_strategy="enable",
            transformation_context_keys=["source_transform", "target_transform"],
        )
        
        assert result["status"] == "success"
        assert "bookmark_configuration" in result
        assert "management_commands" in result
        assert "monitoring_queries" in result
        assert "troubleshooting_guide" in result
        
        config = result["bookmark_configuration"]
        assert config["job_name"] == "test-incremental-job"
        assert config["bookmark_option"] == "job-bookmark-enable"
        assert "source_transform" in config["transformation_contexts"]
        
        commands = result["management_commands"]
        assert "reset_bookmark" in commands
        assert "get_bookmark" in commands
        assert "test-incremental-job" in commands["reset_bookmark"]

    def test_generate_change_data_capture_job_upsert(self):
        """Test CDC job generation with upsert strategy."""
        source_table = DataCatalogTable(
            database_name="operational_db",
            table_name="customer_updates",
        )
        
        target_table = DataCatalogTable(
            database_name="analytics_db",
            table_name="customers",
        )
        
        result = self.glue_integration.generate_change_data_capture_job(
            source_table=source_table,
            target_table=target_table,
            cdc_column="last_modified",
            cdc_strategy="upsert",
            primary_keys=["customer_id"],
        )
        
        assert result["status"] == "success"
        assert result["cdc_strategy"] == "upsert"
        assert result["cdc_column"] == "last_modified"
        assert result["primary_keys"] == ["customer_id"]
        
        job_template = result["job_template"]
        assert "Delta Lake" in job_template or "delta" in job_template
        assert "merge" in job_template
        assert "whenMatchedUpdateAll" in job_template
        assert "whenNotMatchedInsertAll" in job_template

    def test_generate_change_data_capture_job_append(self):
        """Test CDC job generation with append strategy."""
        source_table = DataCatalogTable(
            database_name="logs_db",
            table_name="application_logs",
        )
        
        target_table = DataCatalogTable(
            database_name="warehouse_db",
            table_name="all_logs",
        )
        
        result = self.glue_integration.generate_change_data_capture_job(
            source_table=source_table,
            target_table=target_table,
            cdc_column="log_timestamp",
            cdc_strategy="append",
        )
        
        assert result["status"] == "success"
        assert result["cdc_strategy"] == "append"
        
        job_template = result["job_template"]
        assert "mode(\"append\")" in job_template
        assert "log_timestamp" in job_template

    def test_incremental_monitoring_setup(self):
        """Test incremental monitoring setup generation."""
        monitoring = self.glue_integration._generate_incremental_monitoring_setup()
        
        assert "cloudwatch_metrics" in monitoring
        assert "cloudwatch_alarms" in monitoring
        assert "dashboard_widgets" in monitoring
        
        metrics = monitoring["cloudwatch_metrics"]
        assert "RecordsProcessed" in metrics
        assert "ProcessingLatency" in metrics
        assert "BookmarkUpdateSuccess" in metrics
        
        alarms = monitoring["cloudwatch_alarms"]
        assert len(alarms) >= 2
        assert any(alarm["name"] == "IncrementalProcessingFailure" for alarm in alarms)

    def test_incremental_best_practices(self):
        """Test incremental best practices generation."""
        timestamp_practices = self.glue_integration._generate_incremental_best_practices("timestamp")
        watermark_practices = self.glue_integration._generate_incremental_best_practices("watermark")
        bookmark_practices = self.glue_integration._generate_incremental_best_practices("bookmark")
        
        # Check common practices are included
        for practices in [timestamp_practices, watermark_practices, bookmark_practices]:
            assert any("test incremental logic" in practice for practice in practices)
            assert any("monitor data freshness" in practice for practice in practices)
        
        # Check strategy-specific practices
        assert any("timezone conversions" in practice for practice in timestamp_practices)
        assert any("watermark table" in practice for practice in watermark_practices)
        assert any("bookmark limitations" in practice for practice in bookmark_practices)

    def test_bookmark_troubleshooting_guide(self):
        """Test bookmark troubleshooting guide generation."""
        guide = self.glue_integration._generate_bookmark_troubleshooting_guide()
        
        assert "bookmark_not_advancing" in guide
        assert "duplicate_data_processing" in guide
        assert "bookmark_reset_needed" in guide
        assert "bookmark_corruption" in guide
        
        # Check that solutions are provided
        assert "transformation_ctx" in guide["bookmark_not_advancing"]
        assert "reset-job-bookmark" in guide["bookmark_reset_needed"]

    def test_cdc_monitoring_metrics(self):
        """Test CDC monitoring metrics generation."""
        metrics = self.glue_integration._generate_cdc_monitoring_metrics()
        
        assert "records_processed" in metrics
        assert "processing_latency" in metrics
        assert "change_detection_accuracy" in metrics
        assert "upsert_success_rate" in metrics
        assert "data_freshness" in metrics
        assert "conflict_resolution" in metrics
        
        # Check that descriptions are meaningful
        assert "Number of records" in metrics["records_processed"]
        assert "Time between" in metrics["processing_latency"]

    def test_error_handling_in_incremental_job_generation(self):
        """Test error handling in incremental job generation."""
        source_table = DataCatalogTable(
            database_name="test_db",
            table_name="test_table",
        )
        
        target_table = DataCatalogTable(
            database_name="test_db",
            table_name="test_target",
        )
        
        # Test with invalid strategy
        result = self.glue_integration.generate_incremental_processing_job(
            source_table=source_table,
            target_table=target_table,
            incremental_column="test_col",
            incremental_strategy="invalid_strategy",
        )
        
        assert result["status"] == "error"
        assert "Unsupported incremental strategy" in result["message"]